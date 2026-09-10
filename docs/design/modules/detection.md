# Detection Module

**Location**: `src/adapt/modules/detection/module.py`

---

## Responsibility

The detection module is the **feature extraction boundary**. Its sole job is to transform a 2D reflectivity field into a labelled field where each integer label identifies a spatially coherent convective cell.

It answers one question: *where are the cells in this scan?*

---

## What It Is Not Responsible For

- Computing statistics about cells — that is the analysis module's job
- Tracking cells across time — that is the tracking module's job
- Projecting cell motion — that is the projection module's job
- Deciding which cell is which across scans — pure spatial segmentation only
- Producing anything other than an integer label field

---

## Interface

```
DetectionModule (BaseModule)
├── name:    "detection"
├── inputs:  ["grid_ds_2d", "config"]
└── outputs: ["segmented_ds"]
```

### Input: `grid_ds_2d`

The 2D Cartesian grid from the ingest module. Detection reads the canonical reflectivity field from it.

### Input: `config`

Detection reads:
- `config.segmenter.threshold` — minimum reflectivity to consider (dBZ)
- `config.segmenter.min_cellsize` — minimum cell area in grid points
- `config.segmenter.max_cellsize` — maximum cell area in grid points
- `config.segmenter.closing_kernel` — morphological closing radius (pixels)

### Output: `segmented_ds`

A copy of `grid_ds_2d` with one additional variable: `cell_labels`.

```
segmented_ds
├── dims: (y, x)
├── coords: x (m), y (m)
├── data_vars: (all from grid_ds_2d)
└── data_vars:
    └── cell_labels    # int32, 0 = background, 1..N = cells
```

Labels are ordered by decreasing cell area: label 1 is the largest cell, label N is the smallest. This ordering is stable within a single scan. Cross-scan cell identity is determined by the tracking module, not by label values.

---

## Segmentation Algorithm

```
reflectivity field (float32, dBZ)
        │
        ▼ binary threshold at config.segmenter.threshold
binary mask (bool)
        │
        ▼ morphological closing (disk kernel, radius = closing_kernel)
closed mask (fills small holes and gaps)
        │
        ▼ connected component labelling
raw integer labels
        │
        ▼ size filter (remove < min_cellsize, > max_cellsize grid points)
filtered labels
        │
        ▼ relabel by decreasing area
cell_labels (1 = largest, N = smallest, 0 = background)
```

---

## Output Contract (`detection/contracts.py`)

`assert_segmented(segmented_ds)` verifies:
- `cell_labels` variable exists
- Labels are integer type (uint or int)
- Label 0 is the background (most common value)
- Labels are contiguous integers from 0 to N
- Cells are ordered by decreasing area

---

## Connection to the Rest of the System

```
grid_ds_2d (from ingest)
        ↓
  [ DetectionModule ]
        ↓
segmented_ds ────────────→ [ ProjectionModule ]  (needs current + previous)
             ────────────→ [ AnalysisModule ]
             ────────────→ (stored as analysis2d NetCDF by persistence)
```

The detection module receives no information from any prior scan. Its output is purely a function of the current scan's reflectivity field and the configured thresholds. Cross-scan state is the tracking module's domain.

---

## Design Notes

**Why are labels ordered by area?**

Consistent ordering within a scan makes the output predictable and testable. Given the same reflectivity field and the same thresholds, the same cells always get the same labels. This is not used for tracking (which uses UIDs), but it simplifies inspection, debugging, and unit testing.

**Why no ML-based segmentation here?**

The threshold + morphology approach is physically interpretable, computationally cheap, and produces deterministic outputs. An ML-based segmenter (e.g., U-Net) could be added as a second registered implementation — it would implement `BaseModule` with the same `inputs` and `outputs`, and be swapped in by changing only the registry entry. The rest of the pipeline would be unaware of the change.

**Why is closing applied before labelling?**

Morphological closing fills small gaps within a cell caused by precipitation shadows, signal attenuation, or beam geometry. Without it, a single physical storm can produce dozens of disconnected small labels. Closing is a physical correction, not a workaround.
