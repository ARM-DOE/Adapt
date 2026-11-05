# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Decision records: what the segmenter and the tracker considered, and why.

These are analysis-only records written to a separate database
(``decisions.db``). The pipeline never reads them and the public API does not
expose them; they exist so a run can be explained after the fact and gate
thresholds re-tuned offline from stored numbers. Identity (run, scan, time)
is stamped by persistence, never carried here.
"""

from dataclasses import dataclass

from adapt.contracts.pipeline import require

# ── Tracking ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TrackingScan:
    """One scan pair: how it was classified and what each step yielded."""

    dt_s: float | None
    reset_code: str | None  # FIRST_SCAN | NON_MONOTONIC_TIME | TRACK_GAP_EXCEEDED | NO_PROJECTIONS
    n_prev: int
    n_latent: int  # latent tracks offered to this scan
    n_curr: int
    n_pairs: int
    n_pass_overlap: int
    n_pass_speed: int
    n_pass_cost: int
    n_components: int
    n_continue: int
    n_crossing_excluded: int
    n_split: int
    n_merge: int
    n_resumed: int
    n_identity_transfer: int
    n_initiation: int
    n_latent_created: int
    n_termination: int


@dataclass(frozen=True)
class TrackingCell:
    """One current cell: the quantities the decisions used, and its fate.

    ``mass`` is Σe and ``mean_excess`` ē over the cell (e = excess above the
    cell threshold); the centre is the e²-weighted one (metres); ``score`` is
    the identity score S. ``core_area_km2`` is diagnostic only.
    """

    cell_label: int
    cell_uid: str
    area_km2: float
    mass: float
    mean_excess: float
    centre_x: float
    centre_y: float
    score: float
    core_area_km2: float
    fate: str  # CONTINUE | RESUMED | SPLIT_CHILD | INITIATION


@dataclass(frozen=True)
class TrackingPair:
    """One candidate pair: a footprint (live or latent) against a current cell.

    Every quantity is computed for every pair; ``gate`` names the first check
    it failed (OVERLAP, SPEED, SPEED_CHANGE, COST) or PASS. ``outcome`` is
    CONTINUE, RESUMED, LOST (passed, not chosen), CROSSING (excluded because it
    crossed ``crossing_with_uid``'s link) or REJECTED (failed a gate).
    """

    prev_cell_uid: str
    prev_cell_label: int
    curr_cell_label: int
    source: str  # LIVE | LATENT
    steps: int  # scan intervals the link spans
    growth_radius_km: float
    footprint_area_px: int
    footprint_grown_px: int
    cell_grown_px: int
    intersection_px: int
    o_c: float
    o_h: float
    u: float
    predicted_x: float
    predicted_y: float
    curr_centre_x: float
    curr_centre_y: float
    d: float
    speed_ms: float
    prev_speed_ms: float | None
    speed_limit_ms: float | None
    heading_change_deg: float | None
    h: float
    cost: float
    gate: str
    component_id: int | None
    cost_rank: int | None
    margin: float | None
    crossing_with_uid: str | None
    outcome: str


@dataclass(frozen=True)
class TrackingLineage:
    """One hypothesis for a cell the assignment left over, and whether it was chosen.

    Current orphans (side ``curr``) weigh RESUME, SPLIT and INITIATION;
    unlinked previous cells (side ``prev``) weigh MERGE and LATENT.
    ``fraction`` is O^c for a split and O^h for a merge; ``cost`` is the
    resumption cost; ``reason`` says why a hypothesis failed, or IDENTITY when
    the identity rule made the assigned cell a split child or merge source.
    """

    side: str
    cell_uid: str
    cell_label: int
    hypothesis: str
    partner_uid: str | None
    partner_label: int | None
    fraction: float | None
    cost: float | None
    threshold: float | None
    passed: bool
    chosen: bool
    reason: str | None


@dataclass(frozen=True)
class TrackingIdentity:
    """One contender for an identity contested by a split or a merge.

    ``identity_uid`` is the identity at stake; ``candidate_label`` is a previous
    cell (merge) or a current cell (split). ``transferred`` is True when the
    winner is not the cell the assignment linked.
    """

    kind: str  # SPLIT | MERGE
    identity_uid: str
    candidate_side: str  # prev | curr
    candidate_label: int
    score: float
    distance: float
    winner: bool
    rule: str  # SCORE | NEAREST
    transferred: bool


@dataclass(frozen=True)
class TrackingLatent:
    """A latent track at this scan: created, carried, resumed or expired."""

    cell_uid: str
    last_label: int
    last_scan_id: str
    age: int  # scans since the last observation
    origin: str  # MERGED | TERMINATED
    merged_into_uid: str | None
    status: str  # CREATED | CARRIED | RESUMED | EXPIRED
    footprint_px: int
    predicted_x: float
    predicted_y: float


@dataclass(frozen=True)
class TrackingDecisions:
    scan: TrackingScan
    cells: tuple[TrackingCell, ...]
    pairs: tuple[TrackingPair, ...]
    lineage: tuple[TrackingLineage, ...]
    identity: tuple[TrackingIdentity, ...]
    latent: tuple[TrackingLatent, ...]


# ── Segmentation ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SegmentationFrame:
    """One scan: mask, seeds, carried claims and what survived the size filter."""

    mask_px: int
    closed_mask_px: int
    n_components: int
    n_independent_seeds: int
    n_carried_claims: int
    n_carried_admitted: int
    n_basins: int
    n_dropped_small: int
    n_dropped_large: int
    n_cells: int
    # Carried-seed basins the minimum-size filter spared (0 when the exemption
    # is off), so the rescue's contribution can be separated from the filter's.
    n_size_exempt: int = 0


@dataclass(frozen=True)
class SegmentationSeed:
    """One watershed marker — independent or carried, admitted or not.

    ``decision`` is SEEDED for markers used, else why a carried claim was not:
    SURVIVOR (its footprint already holds a seed), EXPIRED (past the carry
    bound), OUTSIDE_MASK, NO_DEFICIT, TOO_CLOSE_TO_SEED, TOO_CLOSE_TO_MARKER.
    ``final_label`` is 0 when the size filter dropped the basin.
    """

    marker_id: int | None
    origin: str  # HMAXIMA | CARRIED
    row: int
    col: int
    peak_value: float | None
    plateau_px: int | None
    component_id: int | None
    prior_label: int | None
    prior_age: int | None
    footprint_px: int | None
    clearance_px: float | None
    footprint_holds_seed: bool | None
    component_claims: int | None
    component_seeds: int | None
    component_deficit: int | None
    decision: str
    basin_px: int | None
    final_label: int | None


@dataclass(frozen=True)
class SegmentationComponent:
    """One 8-connected component of the closed convective mask."""

    component_id: int
    area_px: int
    field_max: float
    field_min: float
    n_seeds: int
    n_claims: int
    deficit: int
    n_admitted: int
    n_final_labels: int


@dataclass(frozen=True)
class SegmentationDecisions:
    frame: SegmentationFrame
    seeds: tuple[SegmentationSeed, ...]
    components: tuple[SegmentationComponent, ...]


# ── Bound checks ─────────────────────────────────────────────────────────────


def check_tracking_decisions(decisions: TrackingDecisions) -> None:
    require(
        isinstance(decisions, TrackingDecisions), "tracking_decisions must be TrackingDecisions"
    )
    scan = decisions.scan
    require(
        scan.n_termination <= scan.n_prev + scan.n_latent,
        "tracking_decisions: more terminations than previous and latent tracks",
    )
    require(
        len(decisions.pairs) == scan.n_pairs,
        f"tracking_decisions: {len(decisions.pairs)} pair rows for {scan.n_pairs} pairs",
    )
    require(
        len(decisions.cells) == scan.n_curr,
        f"tracking_decisions: {len(decisions.cells)} cell rows for {scan.n_curr} current cells",
    )


def check_segmentation_decisions(decisions: SegmentationDecisions) -> None:
    require(
        isinstance(decisions, SegmentationDecisions),
        "detection_decisions must be SegmentationDecisions",
    )
    frame = decisions.frame
    require(
        len(decisions.components) == frame.n_components,
        f"detection_decisions: {len(decisions.components)} component rows for "
        f"{frame.n_components} components",
    )
    require(
        frame.n_cells <= frame.n_basins,
        "detection_decisions: more cells than basins",
    )
