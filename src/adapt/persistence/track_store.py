# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""TrackStore — read/write the three track persistence tables.

Tables managed:
- cells_by_scan : one row per active tracked cell per scan (wide canonical table)
- cell_events   : authoritative lineage edges (CONTINUE/SPLIT/MERGE/INITIATION/TERMINATION)
- cell_tracks   : convenience lifecycle summary per cell_uid

A "track" is a single connected chain of cell observations across scans identified by
a stable cell_uid.

The tables live in the collection's products.db: created with bespoke DDL
(composite UNIQUE + retroactive-update SQL the generic writer cannot express)
and frozen through the shared SchemaLedger on the first write — new columns
are rejected forever after; there is no ALTER.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from adapt.persistence.errors import StoreError
from adapt.persistence.products import (
    TableDeclaration,
    _canonical_sqlite_type,
)
from adapt.utils.time import from_scan_iso, to_scan_iso

if TYPE_CHECKING:
    from adapt.persistence.products import SchemaLedger

__all__ = ["TrackStore"]

logger = logging.getLogger(__name__)

# Fixed cells_by_scan columns (name, type) in DDL order; per-run cell_stats
# payload columns are appended at first-frame freeze.
_CBS_FIXED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "TEXT"),
    ("scan_id", "TEXT"),
    ("scan_time", "TEXT"),
    ("cell_label", "INTEGER"),
    ("cell_uid", "TEXT"),
    ("cell_area_sqkm", "REAL"),
    ("cell_centroid_mass_lat", "REAL"),
    ("cell_centroid_mass_lon", "REAL"),
    ("cell_centroid_geom_x", "REAL"),
    ("cell_centroid_geom_y", "REAL"),
    ("radar_reflectivity_max", "REAL"),
    ("radar_reflectivity_mean", "REAL"),
    ("radar_differential_reflectivity_max", "REAL"),
    ("area_40dbz_km2", "REAL"),
    ("n_adjacent_cells", "INTEGER"),
    ("adjacent_cell_uids_json", "TEXT"),
    ("is_initiated_here", "INTEGER"),
    ("is_split_target_here", "INTEGER"),
    ("is_merge_target_here", "INTEGER"),
    ("age_seconds", "REAL"),
    ("is_split_source_here", "INTEGER"),
    ("is_merge_source_here", "INTEGER"),
    ("is_terminated_after_here", "INTEGER"),
    # A merge ends the absorbed cell's track and the survivor keeps its own uid
    # and age, so the longer history would otherwise be unrecoverable from this
    # table. These name the absorbed cell and the lifetime it had reached at
    # this scan (the oldest one, when several merge into the same target).
    # Identity is deliberately NOT transferred — a consumer that wants the
    # longer lineage reconstructs it from these.
    ("merged_from_cell_uid", "TEXT"),
    ("merged_from_age_seconds", "REAL"),
)

_CBS_NOT_NULL = {"run_id", "scan_id", "scan_time", "cell_label", "cell_uid"}

_CELL_EVENTS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "INTEGER"),
    ("run_id", "TEXT"),
    ("source_scan_id", "TEXT"),
    ("target_scan_id", "TEXT"),
    ("source_scan_time", "TEXT"),
    ("target_scan_time", "TEXT"),
    ("event_type", "TEXT"),
    ("source_cell_uid", "TEXT"),
    ("target_cell_uid", "TEXT"),
    ("source_cell_label", "INTEGER"),
    ("target_cell_label", "INTEGER"),
    ("cost", "REAL"),
    ("is_dominant", "INTEGER"),
    ("event_group_id", "TEXT"),
    ("candidate_opc", "REAL"),
    ("candidate_ocp", "REAL"),
    ("candidate_centroid_distance_m", "REAL"),
    ("candidate_speed_ms", "REAL"),
    ("candidate_heading_change_deg", "REAL"),
    ("candidate_area_ratio", "REAL"),
    ("candidate_final_cost", "REAL"),
    ("match_method", "TEXT"),
)

_CELL_TRACKS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "TEXT"),
    ("cell_uid", "TEXT"),
    ("first_seen_time", "TEXT"),
    ("last_seen_time", "TEXT"),
    ("n_scans", "INTEGER"),
    ("origin_type", "TEXT"),
    ("origin_event_group_id", "TEXT"),
    ("origin_n_parents", "INTEGER"),
    ("origin_primary_parent_cell_uid", "TEXT"),
    ("termination_type", "TEXT"),
    ("termination_event_group_id", "TEXT"),
    ("terminated_into_cell_uid", "TEXT"),
    ("duration_seconds", "REAL"),
    ("max_area_sqkm", "REAL"),
    ("max_reflectivity", "REAL"),
)

_CELL_EVENTS_DDL = """
CREATE TABLE cell_events (
    event_id           INTEGER PRIMARY KEY,
    run_id             TEXT NOT NULL,
    source_scan_id     TEXT,
    target_scan_id     TEXT,
    source_scan_time   TEXT,
    target_scan_time   TEXT,
    event_type         TEXT NOT NULL,
    source_cell_uid    TEXT,
    target_cell_uid    TEXT,
    source_cell_label  INTEGER,
    target_cell_label  INTEGER,
    cost               REAL,
    is_dominant        INTEGER NOT NULL DEFAULT 0,
    event_group_id     TEXT NOT NULL,
    candidate_opc                      REAL,
    candidate_ocp                      REAL,
    candidate_centroid_distance_m      REAL,
    candidate_speed_ms                 REAL,
    candidate_heading_change_deg       REAL,
    candidate_area_ratio               REAL,
    candidate_final_cost               REAL,
    match_method                       TEXT
)
"""

_CELL_TRACKS_DDL = """
CREATE TABLE cell_tracks (
    run_id                          TEXT NOT NULL,
    cell_uid                        TEXT NOT NULL,
    first_seen_time                 TEXT NOT NULL,
    last_seen_time                  TEXT NOT NULL,
    n_scans                         INTEGER NOT NULL DEFAULT 0,
    origin_type                     TEXT NOT NULL,
    origin_event_group_id           TEXT,
    origin_n_parents                INTEGER NOT NULL DEFAULT 0,
    origin_primary_parent_cell_uid  TEXT,
    termination_type                TEXT NOT NULL DEFAULT 'ACTIVE_AT_END',
    termination_event_group_id      TEXT,
    terminated_into_cell_uid        TEXT,
    duration_seconds                REAL NOT NULL DEFAULT 0,
    max_area_sqkm                   REAL,
    max_reflectivity                REAL,
    PRIMARY KEY (run_id, cell_uid)
)
"""

_TRACK_INDEX_DDL = (
    "CREATE INDEX idx_cbs_track ON cells_by_scan(run_id, cell_uid, scan_time)",
    "CREATE INDEX idx_cbs_scan  ON cells_by_scan(run_id, scan_id)",
    "CREATE INDEX idx_cbs_time  ON cells_by_scan(run_id, scan_time)",
    "CREATE INDEX idx_cbs_label ON cells_by_scan(run_id, cell_label, scan_time)",
    "CREATE INDEX idx_ce_source ON cell_events(run_id, source_cell_uid)",
    "CREATE INDEX idx_ce_target ON cell_events(run_id, target_cell_uid)",
    "CREATE INDEX idx_ce_group  ON cell_events(run_id, event_group_id)",
    "CREATE INDEX idx_cell_tracks_run ON cell_tracks(run_id)",
)


_SKIP_FROM_CELL_STATS = {
    # tracked internally by tracked_cells with different names; avoid duplicate writes
    "time",
    "time_volume_start",
}


def _uid_col(df: pd.DataFrame) -> str:
    if "cell_uid" in df.columns:
        return "cell_uid"
    raise ValueError("Missing persistent ID column: expected 'cell_uid'")


def _source_uid(ev: pd.Series):
    return ev.get("source_cell_uid")


def _target_uid(ev: pd.Series):
    return ev.get("target_cell_uid")


def _event_source_scan(ev: pd.Series, prev: tuple[str, str] | None) -> tuple[str, str] | None:
    """(scan_id, scan_time) the source cell was last observed in.

    The previous scan, unless the event names its own — a resumed or an
    expiring latent track refers back to the scan it vanished after.
    """
    scan_id = ev.get("source_scan_id")
    if scan_id is None or pd.isna(scan_id) or scan_id == (prev[0] if prev else None):
        return prev
    return str(scan_id), _to_iso(pd.Timestamp(ev["source_scan_time"]).to_pydatetime())


class TrackStore:
    """Read/write track persistence tables in a collection's products.db.

    Thread-safe via SQLite WAL mode and an internal lock.
    """

    def __init__(self, db_path: Path, ledger: SchemaLedger):
        self._db_path = Path(db_path)
        self._ledger = ledger
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def __enter__(self) -> TrackStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
                isolation_level="DEFERRED",
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write_scan(
        self,
        run_id: str,
        scan_time: datetime,
        cell_stats_df: pd.DataFrame,
        tracked_cells_df: pd.DataFrame,
        cell_events_df: pd.DataFrame,
        cell_adjacency_df: pd.DataFrame,
        *,
        scan_id: str,
    ) -> None:
        """Persist one scan's track outputs to the three tables.

        Parameters
        ----------
        run_id           : pipeline run identifier
        scan_time        : UTC datetime of this scan (ordering/display metadata)
        cell_stats_df    : full analysis output (all cell_stats columns)
        tracked_cells_df : tracking module output (cell_uid, cell_label)
        cell_events_df   : tracking module output (lineage events)
        cell_adjacency_df: analysis module output (label-space adjacency)
        scan_id          : content-derived scan identity (the join key)
        """
        if not scan_id or not isinstance(scan_id, str):
            raise ValueError(f"scan_id is required and must be a non-empty string, got {scan_id!r}")
        if tracked_cells_df.empty:
            return
        if cell_adjacency_df is None:
            raise ValueError("cell_adjacency_df is required (no fallback)")
        if not isinstance(cell_adjacency_df, pd.DataFrame):
            raise TypeError(f"cell_adjacency_df must be a DataFrame, got {type(cell_adjacency_df)}")

        scan_iso = _to_iso(scan_time)
        conn = self._connect()

        with self._lock:
            # 1. First write freezes the schema; new columns are rejected after.
            self._ensure_frozen(conn, cell_stats_df)

            # 1b. Fetch first_seen_time for all active tracks (age computation)
            uid_col = _uid_col(tracked_cells_df)
            self._reject_duplicate_identities(scan_id, tracked_cells_df, uid_col)
            cell_uids = tracked_cells_df[uid_col].astype(str).unique().tolist()
            # A merge source has just died, so it is absent from tracked_cells_df
            # — but its first_seen_time is exactly what merged_from_age_seconds
            # needs, so pull those uids in too.
            if not cell_events_df.empty and "source_cell_uid" in cell_events_df:
                merge_sources = cell_events_df.loc[
                    cell_events_df["event_type"] == "MERGE", "source_cell_uid"
                ].dropna()
                cell_uids += [str(u) for u in merge_sources.unique() if str(u) not in cell_uids]
            placeholders = ",".join("?" * len(cell_uids))
            first_seen_rows = conn.execute(
                "SELECT cell_uid, first_seen_time FROM cell_tracks "
                f"WHERE run_id=? AND cell_uid IN ({placeholders})",
                [run_id] + cell_uids,
            ).fetchall()
            first_seen_map: dict[str, str] = {
                r["cell_uid"]: r["first_seen_time"] for r in first_seen_rows
            }

            adjacency = self._build_uid_adjacency_summary(
                tracked_cells_df=tracked_cells_df,
                cell_adjacency_df=cell_adjacency_df,
            )

            # 2. Build cells_by_scan rows
            rows = self._build_cells_rows(
                run_id,
                scan_id,
                scan_iso,
                cell_stats_df,
                tracked_cells_df,
                cell_events_df,
                adjacency,
                first_seen_map,
            )

            # 3. Upsert cells_by_scan
            self._upsert_cells(conn, rows)

            # 4. Retroactively update previous scan's cells_by_scan flags
            prev = self._prev_scan(conn, run_id, scan_iso)
            if prev and not cell_events_df.empty:
                self._update_retroactive_flags(conn, run_id, prev, cell_events_df)

            # 5. Insert cell_events
            if not cell_events_df.empty:
                self._insert_cell_events(conn, run_id, scan_id, scan_iso, prev, cell_events_df)

            # 6. Upsert cell_tracks summary
            self._upsert_cell_tracks(conn, run_id, scan_iso, tracked_cells_df, cell_events_df)

            conn.commit()
            # Checkpoint WAL so readonly readers using immutable=1 see current data.
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_cells_by_scan(self, run_id: str, scan_id: str) -> pd.DataFrame:
        """All cells_by_scan rows for one scan, keyed by identity."""
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM cells_by_scan WHERE run_id=? AND scan_id=?",
                (run_id, scan_id),
            ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    def get_track_history(self, run_id: str, cell_uid: str) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM cells_by_scan WHERE run_id=? AND cell_uid=? ORDER BY scan_time",
                (run_id, cell_uid),
            ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    def get_cell_events(self, run_id: str, cell_uid: str | None = None) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            if cell_uid is None:
                rows = conn.execute(
                    "SELECT * FROM cell_events WHERE run_id=? ORDER BY event_id",
                    (run_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM cell_events WHERE run_id=? "
                    "AND (source_cell_uid=? OR target_cell_uid=?) ORDER BY event_id",
                    (run_id, cell_uid, cell_uid),
                ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    def get_cell_tracks(self, run_id: str) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM cell_tracks WHERE run_id=? ORDER BY first_seen_time",
                (run_id,),
            ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_uid_adjacency_summary(
        self,
        tracked_cells_df: pd.DataFrame,
        cell_adjacency_df: pd.DataFrame,
    ) -> dict[str, tuple[int, str]]:
        """Translate label-space adjacency to per-cell_uid adjacency summary.

        Returns mapping: cell_uid -> (n_adjacent_cells, adjacent_cell_uids_json).
        """
        import json

        if tracked_cells_df.empty:
            raise ValueError("tracked_cells_df is empty (cannot build adjacency summary)")

        required = {"cell_label_a", "cell_label_b", "touching_boundary_pixels"}
        missing = sorted(required - set(cell_adjacency_df.columns))
        if missing:
            raise ValueError(f"cell_adjacency_df missing required columns: {missing}")

        label_to_uid: dict[int, str] = {}
        for _, r in tracked_cells_df.iterrows():
            lbl = int(r["cell_label"])
            uid = str(r["cell_uid"])
            if lbl in label_to_uid and label_to_uid[lbl] != uid:
                raise ValueError(
                    f"Non-unique mapping for cell_label={lbl}: {label_to_uid[lbl]} vs {uid}"
                )
            label_to_uid[lbl] = uid

        neighbors: dict[str, set[str]] = {uid: set() for uid in label_to_uid.values()}
        for _, row in cell_adjacency_df.iterrows():
            a = int(row["cell_label_a"])
            b = int(row["cell_label_b"])
            if a == b:
                raise ValueError("cell_adjacency_df contains a self-pair")
            if a not in label_to_uid or b not in label_to_uid:
                raise ValueError(f"cell_adjacency_df references unknown cell labels: {a}, {b}")
            ua = label_to_uid[a]
            ub = label_to_uid[b]
            neighbors.setdefault(ua, set()).add(ub)
            neighbors.setdefault(ub, set()).add(ua)

        out: dict[str, tuple[int, str]] = {}
        for uid, adj in neighbors.items():
            ids = sorted(adj)
            out[uid] = (len(ids), json.dumps(ids))
        return out

    @staticmethod
    def _reject_duplicate_identities(
        scan_id: str, tracked_cells_df: pd.DataFrame, uid_col: str
    ) -> None:
        """Refuse a batch whose cell identities collide within one scan.

        Under upsert, a duplicate (scan, cell_uid) or (scan, cell_label) pair
        silently overwrites the earlier row — an observation is lost before any
        constraint fires (issue #72). Identity collisions must surface loudly.
        """
        uids = tracked_cells_df[uid_col].astype(str)
        dup_uids = sorted(uids[uids.duplicated()].unique())
        labels = tracked_cells_df["cell_label"].astype(int)
        dup_labels = sorted(labels[labels.duplicated()].unique())
        if dup_uids or dup_labels:
            raise ValueError(
                f"duplicate cell identity within scan {scan_id}: "
                f"cell_uid={dup_uids}, cell_label={dup_labels} — refusing to "
                "upsert (an observation would be silently overwritten)"
            )

    def _ensure_frozen(self, conn: sqlite3.Connection, cell_stats_df: pd.DataFrame) -> None:
        """First tracking write freezes all three tables; new columns raise forever after."""
        frozen = self._ledger.frozen("cells_by_scan")
        if frozen is not None:
            frozen_names = {name for name, _ in frozen.columns}
            novel = sorted(
                c
                for c in cell_stats_df.columns
                if c not in _SKIP_FROM_CELL_STATS and c not in frozen_names
            )
            if novel:
                raise StoreError(
                    f"cells_by_scan: column(s) {', '.join(novel)} are not in the frozen "
                    "first-frame schema — module outputs may not add columns mid-run"
                )
            return

        extra = tuple(
            (c, _canonical_sqlite_type(c, cell_stats_df[c]))
            for c in cell_stats_df.columns
            if c not in _SKIP_FROM_CELL_STATS and c not in {name for name, _ in _CBS_FIXED_COLUMNS}
        )
        cbs_columns = _CBS_FIXED_COLUMNS + extra
        col_defs = ", ".join(
            f"{name} {sql_type}{' NOT NULL' if name in _CBS_NOT_NULL else ''}"
            for name, sql_type in cbs_columns
        )
        conn.execute(
            f"CREATE TABLE cells_by_scan ({col_defs}, "
            "PRIMARY KEY (run_id, scan_id, cell_uid), UNIQUE (run_id, scan_id, cell_label))"
        )
        conn.execute(_CELL_EVENTS_DDL)
        conn.execute(_CELL_TRACKS_DDL)
        for ddl in _TRACK_INDEX_DDL:
            conn.execute(ddl)
        conn.commit()

        for decl in (
            TableDeclaration(
                table="cells_by_scan",
                owner_module="tracking",
                granularity="scan",
                primary_key=("run_id", "scan_id", "cell_uid"),
                index_columns=("cell_uid", "scan_time", "cell_label"),
                columns=cbs_columns,
            ),
            TableDeclaration(
                table="cell_events",
                owner_module="tracking",
                granularity="run",
                primary_key=("event_id",),
                index_columns=("source_cell_uid", "target_cell_uid", "event_group_id"),
                columns=_CELL_EVENTS_COLUMNS,
            ),
            TableDeclaration(
                table="cell_tracks",
                owner_module="tracking",
                granularity="run",
                primary_key=("run_id", "cell_uid"),
                index_columns=(),
                columns=_CELL_TRACKS_COLUMNS,
            ),
        ):
            self._ledger.register_external(decl)

    def _build_cells_rows(
        self,
        run_id: str,
        scan_id: str,
        scan_iso: str,
        cell_stats_df: pd.DataFrame,
        tracked_cells_df: pd.DataFrame,
        cell_events_df: pd.DataFrame,
        adjacency: dict[str, tuple[int, str]],
        first_seen_map: dict[str, str] | None = None,
    ) -> list[dict]:
        # Index cell_stats by cell_label for O(1) lookup
        stats_map = {int(r["cell_label"]): r for _, r in cell_stats_df.iterrows()}

        # Forward flags from current scan events
        initiated = set()
        split_targets = set()
        merge_targets = set()
        if not cell_events_df.empty:
            for _, ev in cell_events_df.iterrows():
                etype = ev["event_type"]
                tcl = ev.get("target_cell_label")
                if etype == "INITIATION" and pd.notna(tcl):
                    initiated.add(int(tcl))
                elif etype == "SPLIT" and pd.notna(tcl):
                    split_targets.add(int(tcl))
                elif etype == "MERGE" and pd.notna(tcl):
                    merge_targets.add(int(tcl))

        # Parse current scan time once for age computation
        try:
            scan_dt = from_scan_iso(scan_iso)
        except ValueError:
            scan_dt = None

        def _age_at_scan(uid: str) -> float | None:
            """Lifetime a track had reached by this scan, from its first_seen time."""
            if scan_dt is None or not first_seen_map or uid not in first_seen_map:
                return None
            try:
                return max(0.0, (scan_dt - from_scan_iso(first_seen_map[uid])).total_seconds())
            except ValueError:
                return None

        # Absorbed-cell provenance per merge target: keep the oldest source when
        # several merge into the same survivor, since that is the history a
        # consumer would otherwise lose.
        merged_from: dict[int, tuple[str, float | None]] = {}
        if not cell_events_df.empty:
            for _, ev in cell_events_df.iterrows():
                if ev["event_type"] != "MERGE":
                    continue
                tcl, suid = ev.get("target_cell_label"), ev.get("source_cell_uid")
                if pd.isna(tcl) or not suid or pd.isna(suid):
                    continue
                age = _age_at_scan(str(suid))
                prev = merged_from.get(int(tcl))
                if prev is None or (age or 0.0) > (prev[1] or 0.0):
                    merged_from[int(tcl)] = (str(suid), age)

        rows = []
        uid_col = _uid_col(tracked_cells_df)
        for _, tc in tracked_cells_df.iterrows():
            cl = int(tc["cell_label"])
            tid = str(tc[uid_col])

            # Compute age_seconds from first_seen_time (0 for new initiations)
            age_seconds = 0.0
            if (
                scan_dt is not None
                and cl not in initiated
                and first_seen_map
                and tid in first_seen_map
            ):
                try:
                    first_dt = from_scan_iso(first_seen_map[tid])
                    age_seconds = max(0.0, (scan_dt - first_dt).total_seconds())
                except ValueError:
                    pass

            row: dict = {
                "run_id": run_id,
                "scan_id": scan_id,
                "scan_time": scan_iso,
                "cell_label": cl,
                "cell_uid": tid,
                "age_seconds": age_seconds,
                "n_adjacent_cells": int(adjacency.get(tid, (0, "[]"))[0]),
                "adjacent_cell_uids_json": adjacency.get(tid, (0, "[]"))[1],
                "is_initiated_here": int(cl in initiated),
                "is_split_target_here": int(cl in split_targets),
                "is_merge_target_here": int(cl in merge_targets),
                "is_split_source_here": 0,
                "is_merge_source_here": 0,
                "is_terminated_after_here": 0,
                "merged_from_cell_uid": merged_from.get(cl, (None, None))[0],
                "merged_from_age_seconds": merged_from.get(cl, (None, None))[1],
            }
            # Merge all cell_stats columns
            if cl in stats_map:
                for col, val in stats_map[cl].items():
                    if col in _SKIP_FROM_CELL_STATS or col == "cell_label":
                        continue
                    row.setdefault(col, None if pd.isna(val) else val)
            rows.append(row)
        return rows

    def _upsert_cells(self, conn: sqlite3.Connection, rows: list[dict]) -> None:
        if not rows:
            return
        # Column set is the union over ALL rows (first-seen order): stats
        # columns are merged per cell, so rows within one scan can carry
        # different key sets — keying off rows[0] alone either KeyErrors
        # (first row has stats, later one doesn't) or silently drops
        # columns (the reverse). NULL for a column a row lacks is correct
        # here: the frozen schema already validated the full column set.
        cols: list[str] = []
        for r in rows:
            for k in r:
                if k not in cols:
                    cols.append(k)
        placeholders = ", ".join("?" * len(cols))
        col_list = ", ".join(cols)
        update_set = ", ".join(
            f"{c}=excluded.{c}" for c in cols if c not in ("run_id", "scan_id", "cell_uid")
        )
        sql = (
            f"INSERT INTO cells_by_scan ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(run_id, scan_id, cell_uid) DO UPDATE SET {update_set}"
        )
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])

    def _prev_scan(
        self, conn: sqlite3.Connection, run_id: str, scan_iso: str
    ) -> tuple[str, str] | None:
        """(scan_id, scan_time) of the latest earlier scan; ordering is time-based."""
        row = conn.execute(
            "SELECT scan_id, scan_time FROM cells_by_scan "
            "WHERE run_id=? AND scan_time<? ORDER BY scan_time DESC LIMIT 1",
            (run_id, scan_iso),
        ).fetchone()
        return (row["scan_id"], row["scan_time"]) if row else None

    def _update_retroactive_flags(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        prev: tuple[str, str],
        cell_events_df: pd.DataFrame,
    ) -> None:
        """Set is_split_source, is_merge_source, is_terminated_after on the source scan's rows."""
        flags = {
            "TERMINATION": "is_terminated_after_here",
            "SPLIT": "is_split_source_here",
            "MERGE": "is_merge_source_here",
        }
        for _, ev in cell_events_df.iterrows():
            flag, stid = flags.get(str(ev["event_type"])), _source_uid(ev)
            if flag is None or pd.isna(stid):
                continue
            source = _event_source_scan(ev, prev)
            if source is None:
                continue
            conn.execute(
                f"UPDATE cells_by_scan SET {flag}=1 WHERE run_id=? AND scan_id=? AND cell_uid=?",
                (run_id, source[0], str(stid)),
            )

    def _insert_cell_events(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        scan_id: str,
        target_iso: str,
        prev: tuple[str, str] | None,
        cell_events_df: pd.DataFrame,
    ) -> None:
        cols = [
            "run_id",
            "source_scan_id",
            "target_scan_id",
            "source_scan_time",
            "target_scan_time",
            "event_type",
            "source_cell_uid",
            "target_cell_uid",
            "source_cell_label",
            "target_cell_label",
            "cost",
            "is_dominant",
            "event_group_id",
        ]
        # Per-match diagnostics carried through verbatim when the tracker emits them.
        diagnostic_cols = [
            "candidate_opc",
            "candidate_ocp",
            "candidate_centroid_distance_m",
            "candidate_speed_ms",
            "candidate_heading_change_deg",
            "candidate_area_ratio",
            "candidate_final_cost",
            "match_method",
        ]
        all_cols = cols + diagnostic_cols
        placeholders = ", ".join("?" * len(all_cols))
        sql = f"INSERT INTO cell_events ({', '.join(all_cols)}) VALUES ({placeholders})"

        # No target scan for an event that ends or suspends a track.
        untargeted = {"TERMINATION", "LATENT"}

        def _num(ev: pd.Series, col: str) -> float | None:
            val = ev.get(col)
            return float(val) if pd.notna(val) else None

        rows = []
        for _, ev in cell_events_df.iterrows():
            etype = str(ev["event_type"])
            source_uid = _source_uid(ev)
            target_uid = _target_uid(ev)
            method = ev.get("match_method")
            source = None if etype == "INITIATION" else _event_source_scan(ev, prev)
            rows.append(
                (
                    run_id,
                    source[0] if source else None,
                    None if etype in untargeted else scan_id,
                    source[1] if source else None,
                    None if etype in untargeted else target_iso,
                    etype,
                    source_uid if pd.notna(source_uid) else None,
                    target_uid if pd.notna(target_uid) else None,
                    (
                        int(ev["source_cell_label"])
                        if pd.notna(ev.get("source_cell_label"))
                        else None
                    ),
                    (
                        int(ev["target_cell_label"])
                        if pd.notna(ev.get("target_cell_label"))
                        else None
                    ),
                    float(ev["cost"]) if pd.notna(ev.get("cost")) else None,
                    int(bool(ev.get("is_dominant", False))),
                    str(ev["event_group_id"]),
                    _num(ev, "candidate_opc"),
                    _num(ev, "candidate_ocp"),
                    _num(ev, "candidate_centroid_distance_m"),
                    _num(ev, "candidate_speed_ms"),
                    _num(ev, "candidate_heading_change_deg"),
                    _num(ev, "candidate_area_ratio"),
                    _num(ev, "candidate_final_cost"),
                    str(method) if pd.notna(method) else None,
                )
            )
        conn.executemany(sql, rows)

    def _upsert_cell_tracks(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        scan_iso: str,
        tracked_cells_df: pd.DataFrame,
        cell_events_df: pd.DataFrame,
    ) -> None:
        # Build lookup: cell_uid → (max_area, max_refl)
        active: dict[str, dict] = {}
        uid_col = _uid_col(tracked_cells_df)
        for _, tc in tracked_cells_df.iterrows():
            tid = str(tc[uid_col])
            # Strict reads: these columns are part of the tracked_cells
            # contract; a missing one is an upstream bug and a silent 0.0
            # here would corrupt every lifecycle summary undetected.
            active[tid] = {
                "area": float(tc["area"]),
                "refl": float(tc["max_reflectivity"]),
            }

        # Classify events for origin/termination
        initiated: dict[str, str] = {}  # cell_uid → event_group_id
        split_children: dict[str, tuple[str, str]] = {}  # child_tid → (parent_tid, group_id)
        terminated: dict[str, str] = {}  # cell_uid → event_group_id
        merged_into: dict[str, tuple[str, str]] = {}  # src_tid → (tgt_tid, group_id)

        if not cell_events_df.empty:
            for _, ev in cell_events_df.iterrows():
                etype = str(ev["event_type"])
                gid = str(ev["event_group_id"])
                stid = _source_uid(ev)
                ttid = _target_uid(ev)
                if etype == "INITIATION" and pd.notna(ttid):
                    initiated[str(ttid)] = gid
                elif etype == "SPLIT" and pd.notna(ttid) and pd.notna(stid):
                    split_children[str(ttid)] = (str(stid), gid)
                elif etype == "TERMINATION" and pd.notna(stid):
                    terminated[str(stid)] = gid
                    if pd.notna(ttid):  # a merged-away track ending: target is the survivor
                        merged_into[str(stid)] = (str(ttid), gid)
                elif etype == "MERGE" and pd.notna(stid) and pd.notna(ttid):
                    merged_into[str(stid)] = (str(ttid), gid)

        # Existing tracks in DB
        existing = {
            r["cell_uid"]: dict(r)
            for r in conn.execute(
                "SELECT cell_uid, n_scans, max_area_sqkm, max_reflectivity "
                "FROM cell_tracks WHERE run_id=?",
                (run_id,),
            ).fetchall()
        }

        for tid, info in active.items():
            if tid in existing:
                conn.execute(
                    """UPDATE cell_tracks SET
                        last_seen_time=?,
                        n_scans=n_scans+1,
                        duration_seconds=(julianday(?)-julianday(first_seen_time))*86400.0,
                        max_area_sqkm=MAX(COALESCE(max_area_sqkm,0), ?),
                        max_reflectivity=MAX(COALESCE(max_reflectivity,0), ?)
                    WHERE run_id=? AND cell_uid=?""",
                    (scan_iso, scan_iso, info["area"], info["refl"], run_id, tid),
                )
            else:
                # Determine origin
                if tid in initiated:
                    origin_type = "INITIATION"
                    origin_grp = initiated[tid]
                    origin_n = 0
                    origin_parent = None
                elif tid in split_children:
                    origin_type = "SPLIT"
                    origin_grp = split_children[tid][1]
                    origin_n = 1
                    origin_parent = split_children[tid][0]
                else:
                    origin_type = "UNKNOWN"
                    origin_grp = None
                    origin_n = 0
                    origin_parent = None

                conn.execute(
                    """INSERT INTO cell_tracks
                        (run_id, cell_uid, first_seen_time, last_seen_time,
                         n_scans, origin_type, origin_event_group_id, origin_n_parents,
                         origin_primary_parent_cell_uid, termination_type,
                         max_area_sqkm, max_reflectivity)
                    VALUES (?,?,?, ?,1,?,?,?,?,'ACTIVE_AT_END',?,?)
                    ON CONFLICT(run_id, cell_uid) DO UPDATE SET
                        last_seen_time=excluded.last_seen_time,
                        n_scans=cell_tracks.n_scans+1,
                        duration_seconds=(
                            julianday(excluded.last_seen_time)
                            - julianday(cell_tracks.first_seen_time)
                        )*86400.0,
                        max_area_sqkm=MAX(
                            COALESCE(cell_tracks.max_area_sqkm,0), excluded.max_area_sqkm
                        ),
                        max_reflectivity=MAX(
                            COALESCE(cell_tracks.max_reflectivity,0), excluded.max_reflectivity
                        )""",
                    (
                        run_id,
                        tid,
                        scan_iso,
                        scan_iso,
                        origin_type,
                        origin_grp,
                        origin_n,
                        origin_parent,
                        info["area"],
                        info["refl"],
                    ),
                )

        # Update termination for tracks not in this scan
        for tid, gid in terminated.items():
            if tid not in active:
                if tid in merged_into:
                    tgt_tid, merge_gid = merged_into[tid]
                    conn.execute(
                        """UPDATE cell_tracks SET termination_type='MERGED',
                            termination_event_group_id=?, terminated_into_cell_uid=?
                        WHERE run_id=? AND cell_uid=?""",
                        (merge_gid, tgt_tid, run_id, tid),
                    )
                else:
                    conn.execute(
                        """UPDATE cell_tracks SET termination_type='TERMINATION',
                            termination_event_group_id=?
                        WHERE run_id=? AND cell_uid=?""",
                        (gid, run_id, tid),
                    )


# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------


def _to_iso(dt: datetime) -> str:
    # Single authoritative scan-time format lives in adapt.utils.time.to_scan_iso
    # so cells_by_scan and every derived module table share one join-key string.
    return to_scan_iso(dt)
