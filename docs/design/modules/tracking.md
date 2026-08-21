# Tracking Module

**Location**: `src/adapt/modules/tracking/module.py`

---

## Responsibility

The tracking module is the **cell identity boundary**. Its job is to assign stable, unique identifiers to cells across consecutive scans, linking each cell to its predecessor(s) and recording the nature of that link (continuation, split, merge, initiation, or termination).

It answers: *which cell in this scan corresponds to which cell in the previous scan, and what happened between them?*

---

## What It Is Not Responsible For

- Computing cell statistics — that is the analysis module's job
- Computing motion vectors — that is the projection module's job
- Segmenting or labelling cells — that is the detection module's job
- Storing tracking results to disk — that is the persistence layer's job (`TrackStore`)
- Making decisions about which frames to compare — that is the runtime processor's job

---

## Interface

```
TrackingModule (BaseModule)
├── name:    "tracking"
├── inputs:  ["cell_stats", "config"]
└── outputs: ["tracked_cells", "cell_events"]
```

### Input: `cell_stats`

The per-cell statistics DataFrame from the analysis module. Tracking reads centroid positions (both current and projected), cell area, and scan time. It does not read raw label arrays.

### Input: `config`

Tracking reads:
- `config.tracker.overlap_threshold` — minimum projected mask overlap fraction to consider a match
- `config.tracker.assignment_method` — cost minimisation strategy (default: linear sum assignment)
- `config.tracker.max_search_radius` — maximum distance between projected and observed centroids

### Output: `tracked_cells`

`cell_stats` with additional tracking columns appended. Every cell that was successfully linked receives a `cell_uid`.

```
Additional columns beyond cell_stats:

cell_uid           str      — stable hash identifier (persists across scans)
age_seconds        float    — time since this cell_uid was first seen
is_initiated_here  bool     — True if this is the first scan for this uid
is_continued_from  bool     — True if linked to exactly one predecessor
is_split_source    bool     — True if this uid split from another
is_merge_target    bool     — True if this uid merged from multiple sources
is_terminated_after bool    — True if this uid is not seen in the next scan
```

### Output: `cell_events`

A separate DataFrame recording the directed lineage graph between cells across scans.

```
cell_events columns:

time              datetime — scan time of the child cell
event_type        str      — CONTINUE | SPLIT | MERGE | INITIATION | TERMINATION
source_cell_uid   str      — parent cell UID (null for INITIATION)
target_cell_uid   str      — child cell UID (null for TERMINATION)
source_cell_label int      — parent label in its scan
target_cell_label int      — child label in its scan
cost              float    — assignment cost (lower = better match)
is_dominant       bool     — primary lineage edge for this target cell
event_group_id    str      — groups all events from the same split/merge complex
```

---

## Tracking Algorithm

```
Previous scan's tracked_cells (with UIDs + projected centroids)
Current scan's cell_stats (with observed centroids)
        │
        ▼ Compute overlap matrix
For each (prev_cell, curr_cell) pair:
    - Warp prev projected labels to current time step
    - Compute IoU (intersection over union) of projected vs observed masks
        │
        ▼ Linear sum assignment (scipy.optimize.linear_sum_assignment)
Cost matrix → optimal 1:1 assignment
        │
        ▼ Classify assignments
Assignment cost < threshold → CONTINUE
One prev → many curr → SPLIT events
Many prev → one curr → MERGE events
Unmatched prev → TERMINATION
Unmatched curr → INITIATION
        │
        ▼ Assign UIDs
CONTINUE: inherit parent's UID
SPLIT: generate new UIDs from parent hash
MERGE: inherit dominant parent's UID
INITIATION: generate new UID from cell properties (time, position)
```

**Cell UIDs are deterministic hashes**, not autoincrement integers. Given the same observation (time, position, properties), a cell always gets the same UID. This means UIDs are reproducible across independent pipeline runs on the same data.

---

## Module State

The tracking module maintains a small internal state across scans: the directed tracking graph (`networkx.DiGraph`) that records all cells and events seen so far. This is necessary because cell age requires knowing when a UID was first introduced.

This state is owned by the module instance, which persists across file executions (the same module instance processes all files in sequence). The state is not shared with any other component and is never written directly to disk — the persistence layer reads the outputs (`tracked_cells`, `cell_events`) and stores them.

---

## Output Contracts (`tracking/contracts.py`)

`assert_tracked_cells(tracked_cells)` verifies:
- `cell_uid` column exists and has no nulls
- All UIDs are non-empty strings
- `age_seconds` is non-negative

`assert_cell_events(cell_events)` verifies:
- `event_type` contains only valid values
- `source_cell_uid` is null only for INITIATION events
- `target_cell_uid` is null only for TERMINATION events

---

## Connection to the Rest of the System

```
cell_stats (from analysis)
        ↓
  [ TrackingModule ]
        ↓
tracked_cells ──────────→ [ TrackStore ] (persistence, cells_by_scan table)
              ──────────→ [ GUI / Dashboard ] (live cell display with UIDs)
              ──────────→ [ StoreClient ] (queryable via SQL)
cell_events ────────────→ [ TrackStore ] (persistence, cell_events table)
            ────────────→ [ StoreClient ] (queryable for lineage graph)
```

---

## Design Notes

**Why hash-based UIDs rather than sequential integers?**

Sequential integers are not reproducible. If the pipeline is restarted, the sequence resets and all UIDs from the prior run become ambiguous. Hash-based UIDs derived from observable properties (time of initiation, initial position) are the same regardless of how many times the pipeline runs or in what order files are processed.

**Why does tracking receive `cell_stats` rather than raw label arrays?**

The tracking algorithm operates on centroids and projected positions — not on raw pixel masks. Passing `cell_stats` keeps the interface minimal and decouples tracking from the specific array representations used internally. If the projection module changes its output format, tracking is unaffected as long as `cell_stats` contains the projected centroid columns.

**Why is the event graph recorded separately from `tracked_cells`?**

Tracking has two distinct consumers:
1. The dashboard, which needs per-cell state at each scan time (→ `tracked_cells`)
2. Lineage analysis, which needs the graph of relationships across all scans (→ `cell_events`)

Conflating these into a single table would either bloat the cell record or lose graph information. Separate outputs serve separate consumers cleanly.

**Why use linear sum assignment?**

Linear sum assignment (Hungarian algorithm) finds the globally optimal 1:1 assignment between previous and current cells given a cost matrix. Greedy nearest-neighbour assignment would find locally optimal matches that can be globally suboptimal when cells are close together. The added complexity of the Hungarian algorithm is justified by the correctness requirement.
