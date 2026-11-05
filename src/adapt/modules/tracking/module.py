# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Track segmented cells across consecutive scans (tracker v2).

Per scan pair, in order:

1. Predict: each previous cell's footprint is the cell advected by the flow.
2. Grow footprint and cell by r = r_max (1 − √A/ℓ0)₊; overlap fractions O^c, O^h
   on the grown shapes; u = 1 − √(O^c O^h).
3. Gates (hard): u ≤ u_max, speed ≤ v_max, speed change ≤ additive limit.
4. Cost c = u + w_d d + h (shape-aware residual of the e²-weighted centre,
   heading term); c ≤ c_max.
5. Joint assignment per group of competing cells, no-link at c_max, no crossing.
6. Lineage of what is left (fractions and crossing test; the speed gates bind
   continuations and resumptions only): resumption of a latent track, split child, merge
   source, initiation; unlinked tracks become latent for up to N scans.
7. Identity at splits and merges follows the larger score S = log A + γ log ē.

Causal: a scan's decisions use only that scan and earlier ones, and are never
revised. Every decision is logged (``decisions()``).
"""

import logging
import math
from dataclasses import dataclass, replace
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr

from adapt.contracts import (
    TrackingCell,
    TrackingDecisions,
    TrackingIdentity,
    TrackingLatent,
    TrackingLineage,
)
from adapt.modules.tracking.config import TrackingConfig
from adapt.modules.tracking.decision_log import ScanLog
from adapt.modules.tracking.events import (
    EndPoint,
    LinkDiagnostics,
    build_cell_events_dataframe,
    event_row,
)
from adapt.modules.tracking.identity import _cell_uid_from_signature, track_signature_v2
from adapt.modules.tracking.latent import (
    ORIGIN_MERGED,
    ORIGIN_TERMINATED,
    LatentTrack,
    carry_footprint,
)
from adapt.modules.tracking.lineage import (
    IdentityCandidate,
    best_partner,
    choose_identity,
)
from adapt.modules.tracking.matching.assignment import Link, components, solve_component
from adapt.modules.tracking.matching.gates import drop_crossing_links, segments_cross
from adapt.modules.tracking.matching.geometry import mahalanobis
from adapt.modules.tracking.models import TrackingError
from adapt.modules.tracking.observations import (
    GATE_PASS,
    Cell,
    Footprint,
    Grid,
    Pair,
    Track,
    evaluate_pairs,
    extract_cells,
    latent_footprint,
    live_footprint,
)
from adapt.utils.time import normalize_time_scalar

__all__ = ["CellTracker", "_cell_uid_from_signature", "track_signature_v2"]

logger = logging.getLogger(__name__)

# Beyond this ratio between consecutive scan intervals the cadence is flagged
# irregular (diagnostic only — no track reset).
_CADENCE_IRREGULAR_RATIO = 2.0

Segment = tuple[str, tuple[float, float], tuple[float, float]]  # (uid, start, end)


@dataclass(frozen=True)
class _Interval:
    """Accepted motion of every track, live or latent, over one scan interval."""

    t0_s: float
    t1_s: float
    segments: tuple[Segment, ...]


@dataclass(frozen=True)
class _LastScan:
    """The previous scan: the one every live track was last observed in."""

    scan_id: str
    time: object
    time_s: float


@dataclass
class _Outcome:
    """What one scan pair decided, cell by cell (indices into prev tracks / labels)."""

    continuing: dict[int, int]  # prev idx → curr label
    method: dict[int, str]  # prev idx → ASSIGNED | IDENTITY_MERGE | IDENTITY_SPLIT
    resumed: dict[int, Pair]  # curr label → latent pair
    split: dict[int, int]  # curr label → parent prev idx
    merged: dict[int, int]  # prev idx → survivor curr label


def _epoch_s(time_val) -> float:
    ts = pd.Timestamp(time_val)
    return float((ts.tz_localize("UTC") if ts.tzinfo is None else ts).timestamp())


def _distance(fp: Footprint, cell: Cell, grid: Grid) -> float:
    """Shape-aware distance of a cell's centre from a footprint's predicted centre."""
    if fp.sigma is None:  # a footprint outside the domain is infinitely far
        return math.inf
    return mahalanobis(grid.delta_px(fp.predicted, cell.centre), fp.sigma)


class CellTracker:
    """Track cells across scans; one ``track`` call per scan, in time order."""

    def __init__(self, config: TrackingConfig):
        self.cfg = config
        self._tracks: list[Track] = []
        self._last: _LastScan | None = None
        self._prev_dt_s: float | None = None
        self._latent: list[LatentTrack] = []
        self._intervals: list[_Interval] = []
        self._decisions: TrackingDecisions | None = None

    # ── public API ─────────────────────────────────────────────────────────

    def track(
        self, ds_projected: xr.Dataset, cell_stats_df: pd.DataFrame, *, scan_id: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Process one scan; return its tracked cells and lineage events."""
        if not scan_id:
            raise ValueError("CellTracker.track: scan_id is required")
        time = normalize_time_scalar(ds_projected.time.values)
        time_s = _epoch_s(time)
        grid = Grid.of(ds_projected)
        cells = extract_cells(ds_projected, cell_stats_df, self.cfg, grid)

        reset_code, dt_s = self._reset_code(ds_projected, time_s)
        log = ScanLog(dt_s=dt_s, reset_code=reset_code)
        log.counts.update(prev=len(self._tracks), latent=len(self._latent))
        if reset_code is None:
            uids, events, tracks = self._link(
                self._last_scan(), ds_projected, cells, grid, time, time_s, scan_id, log
            )
        else:
            uids, events, tracks = self._restart(cells, time, time_s, scan_id, log)

        self._tracks = tracks
        self._last = _LastScan(scan_id, time, time_s)
        self._decisions = log.freeze()
        return self._tracked_cells(time, cells, uids), build_cell_events_dataframe(events)

    def decisions(self) -> TrackingDecisions:
        """The decision record of the most recent ``track`` call (analysis only)."""
        if self._decisions is None:
            raise ValueError("CellTracker.decisions: no scan has been tracked yet")
        return self._decisions

    # ── scan classification and restart ────────────────────────────────────

    def _reset_code(self, ds: xr.Dataset, time_s: float) -> tuple[str | None, float | None]:
        if self._last is None:
            return "FIRST_SCAN", None
        dt_s = time_s - self._last.time_s
        if dt_s <= 0:
            code = TrackingError.NON_MONOTONIC_TIME.value
        elif dt_s > self.cfg.max_tracking_gap_minutes * 60.0:
            code = TrackingError.TRACK_GAP_EXCEEDED.value
        elif "cell_projections" not in ds.data_vars:
            code = "NO_PROJECTIONS"
        else:
            if self._prev_dt_s is not None and not (
                1 / _CADENCE_IRREGULAR_RATIO <= dt_s / self._prev_dt_s <= _CADENCE_IRREGULAR_RATIO
            ):
                logger.warning(
                    "tracking_error code=%s dt_s=%.1f prev_dt_s=%.1f",
                    TrackingError.IRREGULAR_SCAN_CADENCE.value,
                    dt_s,
                    self._prev_dt_s,
                )
            self._prev_dt_s = dt_s
            return None, dt_s
        logger.error("tracking_error code=%s dt_s=%.1f — tracks restart", code, dt_s)
        return code, dt_s

    def _restart(self, cells, time, time_s, scan_id, log):
        """End every live and latent track; every current cell starts a track."""
        events = [
            event_row(
                time,
                "TERMINATION",
                EndPoint(t.uid, t.label),
                None,
                source_scan=(t.scan_id, t.scan_time),
            )
            for t in self._tracks
        ] + [
            event_row(
                time,
                "TERMINATION",
                EndPoint(lt.uid, lt.label),
                None,
                source_scan=(lt.scan_id, lt.scan_time),
            )
            for lt in self._latent
        ]
        log.counts["termination"] += len(events)
        self._latent, self._intervals = [], []
        uids = {c.label: self._new_uid(scan_id, c.label) for c in cells}
        for c in cells:
            events.append(event_row(time, "INITIATION", None, EndPoint(uids[c.label], c.label)))
            log.cells[c.label] = self._cell_record(c, uids[c.label], "INITIATION")
        tracks = [self._new_track(c, uids[c.label], scan_id, time, time_s) for c in cells]
        return uids, events, tracks

    # ── one scan pair ──────────────────────────────────────────────────────

    def _last_scan(self) -> _LastScan:
        if self._last is None:
            raise RuntimeError("CellTracker: a scan pair needs a previous scan")
        return self._last

    def _link(self, last: _LastScan, ds, cells, grid, time, time_s, scan_id, log):
        cfg = self.cfg
        labels = ds[cfg.labels_var].values
        by_label = {c.label: c for c in cells}
        proj = np.asarray(ds["cell_projections"].values[0])
        live = [live_footprint(t, proj, grid, time_s - last.time_s, cfg) for t in self._tracks]
        carried = self._carry_latent(ds, grid)
        latent_fps = [latent_footprint(lt, time_s, grid, cfg) for lt in self._latent]

        live_pairs = {
            (i, p.cell.label): p
            for i, fp in enumerate(live)
            for p in evaluate_pairs(fp, by_label, labels, grid, cfg)
        }
        latent_pairs = {
            (k, p.cell.label): p
            for k, fp in enumerate(latent_fps)
            for p in evaluate_pairs(fp, by_label, labels, grid, cfg)
        }
        for p in [*live_pairs.values(), *latent_pairs.values()]:
            log.pair(p.fp.uid, p.cell.label, **self._pair_record(p))

        out = _Outcome({}, {}, {}, {}, {})
        self._assign(live_pairs, out, log)
        current = tuple(
            (live[i].uid, live[i].origin, by_label[j].centre) for i, j in out.continuing.items()
        )
        self._resume(latent_pairs, cells, out, current, last.time_s, time_s, log)
        self._split_and_merge(live, live_pairs, cells, out, current, log)
        self._identity(live, live_pairs, by_label, grid, out, log)
        return self._emit(last, live, live_pairs, cells, out, carried, time, time_s, scan_id, log)

    def _carry_latent(self, ds: xr.Dataset, grid: Grid) -> dict[str, Segment]:
        """Move every latent footprint with this scan's flow; one scan older.

        Returns each latent track's predicted step over this interval.
        """
        flow_x, flow_y = ds["heading_x"].values, ds["heading_y"].values
        carried, steps = [], {}
        for lt in self._latent:
            mask, (d_row, d_col) = carry_footprint(lt.footprint, flow_x, flow_y)
            predicted = (lt.predicted[0] + d_col * grid.sx, lt.predicted[1] + d_row * grid.sy)
            carried.append(replace(lt, footprint=mask, predicted=predicted, age=lt.age + 1))
            steps[lt.uid] = (lt.uid, lt.predicted, predicted)
        self._latent = carried
        return steps

    def _assign(self, pairs: dict[tuple[int, int], Pair], out: _Outcome, log: ScanLog) -> None:
        """Hungarian per component with no-link at c_max, then a scan-wide crossing check."""
        admissible = {e: p for e, p in pairs.items() if p.gate == GATE_PASS}
        chosen: dict[tuple[int, int], Pair] = {}
        groups = components(sorted(admissible))
        log.counts["components"] = len(groups)
        for cid, (prevs, _) in enumerate(groups):
            links = [
                Link(i, j, p.cost, p.fp.origin, p.cell.centre)
                for (i, j), p in sorted(admissible.items())
                if i in prevs
            ]
            solution = solve_component(links, self.cfg.max_link_cost)
            for i, j in (e for e in admissible if e[0] in prevs):
                log.update_pair(pairs[(i, j)].fp.uid, j, component_id=cid, outcome="LOST")
            for e, blocker in solution.crossing_excluded.items():
                self._mark_crossing(pairs, e, blocker, log)
            for e in solution.chosen:
                chosen[e] = admissible[e]
                log.update_pair(admissible[e].fp.uid, e[1], margin=solution.margins[e])
        kept, dropped = drop_crossing_links(
            {e: (p.fp.origin, p.cell.centre, p.cost) for e, p in chosen.items()}
        )
        for e, blocker in dropped.items():
            self._mark_crossing(pairs, e, blocker, log)
        for i, j in kept:
            out.continuing[i] = j
            out.method[i] = "ASSIGNED"
            log.update_pair(pairs[(i, j)].fp.uid, j, outcome="CONTINUE")

    @staticmethod
    def _mark_crossing(pairs, edge, blocker, log: ScanLog) -> None:
        log.counts["crossing_excluded"] += 1
        log.update_pair(
            pairs[edge].fp.uid, edge[1], outcome="CROSSING", crossing_with_uid=pairs[blocker].fp.uid
        )

    def _resume(self, pairs, cells, out: _Outcome, current, prev_s, time_s, log: ScanLog) -> None:
        """Offer latent tracks to orphans: same gates and cost over the elapsed time,
        and no crossing in any intervening interval."""
        orphans = {c.label for c in cells} - set(out.continuing.values())
        admissible: dict[tuple[int, int], Pair] = {}
        for (k, j), p in sorted(pairs.items()):
            if j not in orphans:
                continue
            reason = None if p.gate == GATE_PASS else p.gate
            if reason is None and self._resume_crosses(self._latent[k], p, current, prev_s, time_s):
                reason = "CROSSING"
                log.counts["crossing_excluded"] += 1
                log.update_pair(p.fp.uid, j, outcome="CROSSING")
            if reason is None:
                admissible[(k, j)] = p
            log.lineage.append(self._hypothesis("RESUME", p, cost=p.cost, reason=reason))
        for prevs, _ in components(sorted(admissible)):
            links = [
                Link(k, j, p.cost, p.fp.origin, p.cell.centre)
                for (k, j), p in sorted(admissible.items())
                if k in prevs
            ]
            for k, j in solve_component(links, self.cfg.max_link_cost).chosen:
                out.resumed[j] = admissible[(k, j)]
                log.update_pair(admissible[(k, j)].fp.uid, j, outcome="RESUMED")

    def _resume_crosses(self, lt: LatentTrack, pair: Pair, current, prev_s, time_s) -> bool:
        """Interpolate the resumed path to each intervening scan and test each sub-segment
        against every other track's segment of that interval."""
        start, end, t_start = lt.centre, pair.cell.centre, lt.time_s

        def at(t: float) -> tuple[float, float]:
            f = (t - t_start) / (time_s - t_start)
            return start[0] + f * (end[0] - start[0]), start[1] + f * (end[1] - start[1])

        intervals = [iv for iv in self._intervals if iv.t0_s >= t_start]
        intervals.append(_Interval(prev_s, time_s, current))
        return any(
            uid != lt.uid and segments_cross(at(iv.t0_s), at(iv.t1_s), a, b)
            for iv in intervals
            for uid, a, b in iv.segments
        )

    def _split_and_merge(self, live, pairs, cells, out: _Outcome, current, log: ScanLog) -> None:
        """Split children among remaining orphans; merge sources among unlinked tracks."""
        survivors = {j: i for i, j in out.continuing.items()}
        for j in (
            c.label for c in cells if c.label not in survivors and c.label not in out.resumed
        ):
            tested = {
                i: p for (i, jj), p in sorted(pairs.items()) if jj == j and i in out.continuing
            }
            parent = self._lineage_partner(tested, "SPLIT", current, log)
            if parent is not None:
                out.split[j] = parent
        for i in (i for i in range(len(live)) if i not in out.continuing):
            tested = {j: p for (ii, j), p in sorted(pairs.items()) if ii == i and j in survivors}
            target = self._lineage_partner(tested, "MERGE", current, log)
            if target is not None:
                out.merged[i] = target
            log.lineage.append(self._latent_hypothesis(live[i]))

    def _lineage_partner(self, tested: dict[int, Pair], kind: str, current, log: ScanLog):
        """Best partner by fraction among those passing the crossing test and the
        threshold; every partner tested is logged.

        No speed gate: a fragment's centre moves from its parent's centre by up to
        the parent's size, which is not motion (the speed limits apply to links of
        one cell: continuations and resumptions).
        """
        threshold = self.cfg.split_overlap if kind == "SPLIT" else self.cfg.merge_overlap
        fractions = {}
        for key, p in tested.items():
            fraction = p.overlap.o_c if kind == "SPLIT" else p.overlap.o_h
            reason = None
            if any(
                uid != p.fp.uid and segments_cross(p.fp.origin, p.cell.centre, a, b)
                for uid, a, b in current
            ):
                reason = "CROSSING"
            if reason is None and fraction < threshold:
                reason = "BELOW_THRESHOLD"
            if reason is None:
                fractions[key] = fraction
            log.lineage.append(
                self._hypothesis(kind, p, fraction=fraction, threshold=threshold, reason=reason)
            )
        return best_partner(fractions, threshold)

    @staticmethod
    def _latent_hypothesis(fp: Footprint) -> TrackingLineage:
        """An unlinked track kept latent (chosen unless it merged)."""
        return TrackingLineage(
            "prev", fp.uid, fp.label, "LATENT", None, None, None, None, None, True, False, None
        )

    @staticmethod
    def _hypothesis(
        kind, p: Pair, *, fraction=None, cost=None, threshold=None, reason=None
    ) -> TrackingLineage:
        """A lineage row. Uids that are final only at the end of the scan (the orphan's,
        a merge survivor's) are filled in by ``_finish_lineage``."""
        if kind == "MERGE":
            side, uid, label, partner_uid, partner_label = (
                "prev",
                p.fp.uid,
                p.fp.label,
                None,
                p.cell.label,
            )
        else:
            side, uid, label, partner_uid, partner_label = (
                "curr",
                "",
                p.cell.label,
                p.fp.uid,
                p.fp.label,
            )
        return TrackingLineage(
            side,
            uid,
            label,
            kind,
            partner_uid,
            partner_label,
            fraction,
            cost,
            threshold,
            reason in (None, "IDENTITY"),  # IDENTITY: made so by the identity rule
            False,
            reason,
        )

    def _identity(
        self, live: list[Footprint], pairs, by_label, grid: Grid, out: _Outcome, log: ScanLog
    ):
        """The identity at a merge or split follows the larger score S; within δ_S the
        candidate nearer the predicted position. Only candidates whose link would pass
        the speed gates contend: a continuation always obeys the physical limits."""
        margin = self.cfg.identity_score_margin
        survivors = {j: i for i, j in out.continuing.items()}
        for s in sorted(set(out.merged.values())):
            p = survivors[s]
            contenders = [
                p,
                *sorted(
                    i
                    for i, t in out.merged.items()
                    if t == s and pairs[(i, s)].speed_gate.code is None
                ),
            ]
            if len(contenders) == 1:
                continue
            winner = self._decide(
                "MERGE",
                live[p].uid,
                "prev",
                [
                    (
                        i,
                        self._tracks[i].label,
                        self._tracks[i].score,
                        _distance(live[i], by_label[s], grid),
                    )
                    for i in contenders
                ],
                p,
                margin,
                log,
            )
            if winner != p:
                log.update_pair(live[p].uid, s, outcome="LOST")
                del out.continuing[p], out.merged[winner]
                out.continuing[winner], out.method[winner] = s, "IDENTITY_MERGE"
                out.merged[p] = s
                log.lineage.append(
                    self._hypothesis(
                        "MERGE",
                        pairs[(p, s)],
                        fraction=pairs[(p, s)].overlap.o_h,
                        threshold=self.cfg.merge_overlap,
                        reason="IDENTITY",
                    )
                )
                log.lineage.append(self._latent_hypothesis(live[p]))
        children: dict[int, list[int]] = {}
        for j, parent in out.split.items():
            children.setdefault(parent, []).append(j)
        for parent, kids in sorted(children.items()):
            if parent not in out.continuing:
                continue  # the parent itself merged away: its children stay split children
            c = out.continuing[parent]
            kids = [j for j in kids if pairs[(parent, j)].speed_gate.code is None]
            if not kids:
                continue
            winner = self._decide(
                "SPLIT",
                live[parent].uid,
                "curr",
                [
                    (j, j, by_label[j].score, _distance(live[parent], by_label[j], grid))
                    for j in [c, *sorted(kids)]
                ],
                c,
                margin,
                log,
            )
            if winner != c:
                log.update_pair(live[parent].uid, c, outcome="LOST")
                del out.split[winner]
                out.continuing[parent], out.method[parent] = winner, "IDENTITY_SPLIT"
                out.split[c] = parent
                log.lineage.append(
                    self._hypothesis(
                        "SPLIT",
                        pairs[(parent, c)],
                        fraction=pairs[(parent, c)].overlap.o_c,
                        threshold=self.cfg.split_overlap,
                        reason="IDENTITY",
                    )
                )

    @staticmethod
    def _decide(kind, uid, side, contenders, assigned, margin, log: ScanLog):
        winner, rule = choose_identity(
            [IdentityCandidate(key, score, dist) for key, _, score, dist in contenders], margin
        )
        for key, label, score, dist in contenders:
            log.identity.append(
                TrackingIdentity(
                    kind,
                    uid,
                    side,
                    label,
                    score,
                    dist,
                    key == winner,
                    rule,
                    key == winner and winner != assigned,
                )
            )
        if winner != assigned:
            log.counts["identity_transfer"] += 1
        return winner

    # ── outputs of one scan pair ───────────────────────────────────────────

    def _emit(
        self,
        last: _LastScan,
        live,
        pairs,
        cells,
        out: _Outcome,
        carried,
        time,
        time_s,
        scan_id,
        log,
    ):
        cfg = self.cfg
        by_label = {c.label: c for c in cells}
        uids: dict[int, str] = {}
        events: list[dict] = []
        tracks: list[Track] = []
        segments: list[Segment] = []
        prev_scan = (last.scan_id, last.time)

        for i, j in sorted(out.continuing.items(), key=lambda e: e[1]):
            track, p = self._tracks[i], pairs[(i, j)]
            uids[j] = track.uid
            events.append(
                event_row(
                    time,
                    "CONTINUE",
                    EndPoint(track.uid, track.label),
                    EndPoint(track.uid, j),
                    source_scan=prev_scan,
                    diagnostics=self._diagnostics(p, out.method[i]),
                    is_dominant=True,
                )
            )
            log.update_pair(track.uid, j, outcome="CONTINUE")
            log.cells[j] = self._cell_record(by_label[j], track.uid, "CONTINUE")
            tracks.append(self._moved(by_label[j], track.uid, p, scan_id, time, time_s))
            segments.append((track.uid, track.centre, by_label[j].centre))

        resumed_uids = set()
        for j, p in sorted(out.resumed.items()):
            lt = next(lt for lt in self._latent if lt.uid == p.fp.uid)
            resumed_uids.add(lt.uid)
            uids[j] = lt.uid
            events.append(
                event_row(
                    time,
                    "RESUMED",
                    EndPoint(lt.uid, lt.label),
                    EndPoint(lt.uid, j),
                    source_scan=(lt.scan_id, lt.scan_time),
                    diagnostics=self._diagnostics(p, "RESUMED"),
                    is_dominant=True,
                )
            )
            log.cells[j] = self._cell_record(by_label[j], lt.uid, "RESUMED")
            log.latent.append(self._latent_record(lt, "RESUMED"))
            tracks.append(self._moved(by_label[j], lt.uid, p, scan_id, time, time_s))
            f = (last.time_s - lt.time_s) / (time_s - lt.time_s)
            mid = (
                lt.centre[0] + f * (by_label[j].centre[0] - lt.centre[0]),
                lt.centre[1] + f * (by_label[j].centre[1] - lt.centre[1]),
            )
            segments.append((lt.uid, mid, by_label[j].centre))

        for j in sorted(c.label for c in cells if c.label not in uids):
            uids[j] = self._new_uid(scan_id, j)
            if j in out.split:
                parent = self._tracks[out.split[j]]
                events.append(
                    event_row(
                        time,
                        "SPLIT",
                        EndPoint(parent.uid, parent.label),
                        EndPoint(uids[j], j),
                        source_scan=prev_scan,
                    )
                )
                log.cells[j] = self._cell_record(by_label[j], uids[j], "SPLIT_CHILD")
            else:
                events.append(event_row(time, "INITIATION", None, EndPoint(uids[j], j)))
                log.cells[j] = self._cell_record(by_label[j], uids[j], "INITIATION")
            tracks.append(self._new_track(by_label[j], uids[j], scan_id, time, time_s))

        new_latent: list[LatentTrack] = []
        for i in (i for i in range(len(live)) if i not in out.continuing):
            track, fp = self._tracks[i], live[i]
            survivor = EndPoint(uids[out.merged[i]], out.merged[i]) if i in out.merged else None
            into = survivor.uid if survivor else None
            if survivor:
                log.counts["merge"] += 1
                events.append(
                    event_row(
                        time,
                        "MERGE",
                        EndPoint(track.uid, track.label),
                        survivor,
                        source_scan=prev_scan,
                    )
                )
            if cfg.latent_scans <= 1:
                log.counts["termination"] += 1
                events.append(
                    event_row(
                        time,
                        "TERMINATION",
                        EndPoint(track.uid, track.label),
                        survivor,
                        source_scan=prev_scan,
                    )
                )
                continue
            lt = LatentTrack(
                uid=track.uid,
                label=track.label,
                scan_id=track.scan_id,
                scan_time=track.scan_time,
                time_s=track.time_s,
                centre=track.centre,
                step=track.step,
                speed=track.speed,
                score=track.score,
                footprint=fp.mask,
                predicted=fp.predicted,
                age=1,
                origin=ORIGIN_MERGED if into else ORIGIN_TERMINATED,
                merged_into_uid=into,
            )
            new_latent.append(lt)
            log.counts["latent_created"] += 1
            log.latent.append(self._latent_record(lt, "CREATED"))
            events.append(
                event_row(
                    time, "LATENT", EndPoint(track.uid, track.label), None, source_scan=prev_scan
                )
            )

        for lt in self._latent:
            if lt.uid in resumed_uids:
                continue
            if lt.age >= cfg.latent_scans:
                log.counts["termination"] += 1
                log.latent.append(self._latent_record(lt, "EXPIRED"))
                target = EndPoint(lt.merged_into_uid, None) if lt.merged_into_uid else None
                events.append(
                    event_row(
                        time,
                        "TERMINATION",
                        EndPoint(lt.uid, lt.label),
                        target,
                        source_scan=(lt.scan_id, lt.scan_time),
                    )
                )
            else:
                log.latent.append(self._latent_record(lt, "CARRIED"))
                new_latent.append(lt)
            segments.append(carried[lt.uid])

        segments += [(lt.uid, lt.centre, lt.predicted) for lt in new_latent if lt.age == 1]
        self._record_interval(tuple(segments), last.time_s, time_s)
        self._latent = new_latent
        merged_uids = {self._tracks[i].uid: uids[j] for i, j in out.merged.items()}
        split_parents = {j: self._tracks[i].uid for j, i in out.split.items()}
        self._finish_lineage(log, uids, merged_uids, split_parents)
        return uids, events, tracks

    def _record_interval(self, segments: tuple[Segment, ...], prev_s: float, time_s: float) -> None:
        """Keep the last N−1 intervals' motion for the crossing test of resumed paths."""
        keep = max(self.cfg.latent_scans - 1, 0)
        intervals = [*self._intervals, _Interval(prev_s, time_s, segments)]
        self._intervals = intervals[-keep:] if keep else []

    @staticmethod
    def _finish_lineage(
        log: ScanLog, uids: dict[int, str], merged: dict[str, str], split: dict[int, str]
    ) -> None:
        """Fill the final uids, add each orphan's INITIATION hypothesis, mark what was chosen.

        ``merged``: merge source uid → survivor uid; ``split``: child label → parent uid.
        """
        fates = {label: rec.fate for label, rec in log.cells.items()}
        rows = []
        for r in log.lineage:
            if r.side == "curr":
                chosen = {
                    "RESUME": fates[r.cell_label] == "RESUMED"
                    and uids[r.cell_label] == r.partner_uid,
                    "SPLIT": split.get(r.cell_label) == r.partner_uid,
                }[r.hypothesis]
                rows.append(replace(r, cell_uid=uids[r.cell_label], chosen=chosen))
            elif r.hypothesis == "MERGE":
                into = uids[cast(int, r.partner_label)]
                rows.append(replace(r, partner_uid=into, chosen=merged.get(r.cell_uid) == into))
            else:
                continuing = r.cell_uid in {rec.cell_uid for rec in log.cells.values()}
                rows.append(replace(r, chosen=r.cell_uid not in merged and not continuing))
        for j in sorted(j for j, f in fates.items() if f != "CONTINUE"):
            rows.append(
                TrackingLineage(
                    "curr",
                    uids[j],
                    j,
                    "INITIATION",
                    None,
                    None,
                    None,
                    None,
                    None,
                    True,
                    fates[j] == "INITIATION",
                    None,
                )
            )
        log.lineage[:] = rows

    # ── records ────────────────────────────────────────────────────────────

    def _new_uid(self, scan_id: str, label: int) -> str:
        return _cell_uid_from_signature(track_signature_v2(scan_id, label), self.cfg.uid_width)

    @staticmethod
    def _new_track(cell: Cell, uid: str, scan_id: str, time, time_s: float) -> Track:
        return Track(
            uid, cell.label, scan_id, time, time_s, cell.score, cell.centre, cell.geom, None, None
        )

    @staticmethod
    def _moved(cell: Cell, uid: str, p: Pair, scan_id, time, time_s) -> Track:
        """The track after its link: last step per interval and speed over the link."""
        disp = p.displacement
        step = (disp[0] / p.fp.steps, disp[1] / p.fp.steps)
        return Track(
            uid,
            cell.label,
            scan_id,
            time,
            time_s,
            cell.score,
            cell.centre,
            cell.geom,
            step,
            p.speed,
        )

    @staticmethod
    def _diagnostics(p: Pair, method: str) -> LinkDiagnostics:
        return LinkDiagnostics(
            o_c=p.overlap.o_c,
            o_h=p.overlap.o_h,
            residual_m=p.residual_m,
            speed_ms=p.speed,
            heading_change_deg=p.turn_deg,
            area_ratio=np.count_nonzero(p.cell.mask) / max(1, int(np.count_nonzero(p.fp.mask))),
            cost=p.cost,
            method=method,
        )

    @staticmethod
    def _pair_record(p: Pair) -> dict:
        o = p.overlap
        return {
            "prev_cell_label": p.fp.label,
            "source": p.fp.source,
            "steps": p.fp.steps,
            "growth_radius_km": p.fp.radius_km,
            "footprint_area_px": int(np.count_nonzero(p.fp.mask)),
            "footprint_grown_px": o.footprint_grown_px,
            "cell_grown_px": o.cell_grown_px,
            "intersection_px": o.intersection_px,
            "o_c": o.o_c,
            "o_h": o.o_h,
            "u": o.u,
            "predicted_x": p.fp.predicted[0],
            "predicted_y": p.fp.predicted[1],
            "curr_centre_x": p.cell.centre[0],
            "curr_centre_y": p.cell.centre[1],
            "d": p.d,
            "speed_ms": p.speed,
            "prev_speed_ms": p.fp.speed,
            "speed_limit_ms": p.speed_gate.limit_ms,
            "heading_change_deg": p.turn_deg,
            "h": p.h,
            "cost": p.cost,
            "gate": p.gate,
            "outcome": "REJECTED" if p.gate != GATE_PASS else "LOST",
        }

    @staticmethod
    def _cell_record(c: Cell, uid: str, fate: str) -> TrackingCell:
        return TrackingCell(
            c.label,
            uid,
            c.area_km2,
            c.mass,
            c.mean_excess,
            c.centre[0],
            c.centre[1],
            c.score,
            c.core_area_km2,
            fate,
        )

    @staticmethod
    def _latent_record(lt: LatentTrack, status: str) -> TrackingLatent:
        return TrackingLatent(
            lt.uid,
            lt.label,
            lt.scan_id,
            lt.age,
            lt.origin,
            lt.merged_into_uid,
            status,
            int(np.count_nonzero(lt.footprint)),
            lt.predicted[0],
            lt.predicted[1],
        )

    @staticmethod
    def _tracked_cells(time, cells: list[Cell], uids: dict[int, str]) -> pd.DataFrame:
        rows = [
            {
                "time": pd.Timestamp(time).to_datetime64(),
                "cell_label": c.label,
                "cell_uid": uids[c.label],
                "area": c.area_km2,
                "centroid_x": c.centre[0],
                "centroid_y": c.centre[1],
                "mean_reflectivity": c.mean_field,
                "max_reflectivity": c.max_field,
                "core_area": c.core_area_km2,
            }
            for c in cells
        ]
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["cell_uid", "cell_label"]).reset_index(drop=True)
        return df
