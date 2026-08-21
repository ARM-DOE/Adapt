# Projection Module

**Location**: `src/adapt/modules/projection/module.py`

---

## Responsibility

The projection module is the **motion estimation and nowcasting boundary**. Given two consecutive segmented fields, it computes the bulk motion of the radar echo field using optical flow, and extrapolates each cell's position forward in time by up to N steps.

It answers two questions: *how is the field moving?* and *where will these cells be in the next N scans?*

---

## What It Is Not Responsible For

- Detecting or segmenting cells — that is the detection module's job
- Computing per-cell statistics (area, reflectivity, etc.) — that is the analysis module's job
- Tracking cell identity across scans — that is the tracking module's job
- Determining which two frames to pair — that is the runtime processor's job
- Persisting projection results — that is the persistence layer's job

---

## Interface

```
ProjectionModule (BaseModule)
├── name:    "projection"
├── inputs:  ["segmented_ds", "prev_segmented_ds", "config"]
└── outputs: ["projected_ds"]
```

### Input: `segmented_ds`

The current scan's segmented dataset (from detection module). Projection reads the reflectivity field from it to compute optical flow.

### Input: `prev_segmented_ds`

The immediately preceding scan's segmented dataset. This is the "reference frame" for optical flow. The runtime processor is responsible for providing this: projection never fetches prior frames itself.

### Input: `config`

Projection reads:
- `config.projector.method` — flow estimation method (default: Farneback)
- `config.projector.max_projection_steps` — how many future steps to extrapolate (capped at 10)
- `config.projector.flow_params` — algorithm-specific parameters for the optical flow solver

### Output: `projected_ds`

A copy of `segmented_ds` with three additional variables:

```
projected_ds
├── dims: (y, x) for heading fields
├── dims: (step, y, x) for projections
└── data_vars:
    ├── heading_x          # float32, optical flow u-component (pixels/frame)
    ├── heading_y          # float32, optical flow v-component (pixels/frame)
    └── cell_projections   # int32, label field at each projected step
                           # step 0 = current (registered), step 1..N = future
```

`cell_projections[0]` is the current labels after registration (i.e., spatially aligned with `prev_segmented_ds`). `cell_projections[1]` is where cells are projected to be one scan interval ahead. Steps are in scan-interval units; actual time depends on radar operating mode.

---

## Optical Flow Algorithm

```
prev reflectivity (float32)    current reflectivity (float32)
              │                              │
              └──────────── Farneback ───────┘
                                 │
                           flow field (u, v)
                           (pixels per frame)
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
              heading_x (u)           heading_y (v)
              stored per-pixel        stored per-pixel
                    │                         │
                    └────────── warp ──────────┘
                          cell_labels × (N+1) steps
                                 │
                          cell_projections
                          [step 0 .. step N]
```

The flow field is computed on reflectivity (continuous) rather than labels (discrete) because optical flow requires smooth gradients. Labels are then advected using the computed flow via backward mapping.

---

## Frame Pairing: The Runtime Processor's Responsibility

The projection module requires exactly two consecutive frames. It does not store frame history itself. The runtime processor (`runtime/processor.py`) maintains a two-frame rolling buffer and injects `prev_segmented_ds` into the context before calling the pipeline.

**Consequence**: The projection module is stateless. The same `ProjectionModule.run()` call with the same inputs will always produce the same outputs. Frame state lives in the orchestration layer, not in the module.

---

## Output Contract (`projection/contracts.py`)

`assert_projected(projected_ds)` verifies:
- `heading_x` and `heading_y` fields exist with shape `(y, x)`
- `cell_projections` exists with shape `(step, y, x)` where `step = max_projection_steps + 1`
- No NaN values in heading fields

---

## Connection to the Rest of the System

```
segmented_ds (current, from detection)
prev_segmented_ds (previous, injected by runtime processor)
        ↓
  [ ProjectionModule ]
        ↓
projected_ds ──────────────→ [ AnalysisModule ]
                              (provides projected centroids per cell)
             ──────────────→ (stored to disk by persistence)
             ──────────────→ (rendered by visualization layer)
```

---

## Design Notes

**Why does the module receive `prev_segmented_ds` rather than fetching it from the repository?**

Keeping the module stateless means it is trivially testable (pass any two arrays, verify output) and trivially parallelisable. If projection fetched prior frames from the database, it would acquire an implicit dependency on the persistence layer and on execution order that is invisible from its interface declaration. Explicit is always better.

**Why optical flow rather than cross-correlation or a tracking-based motion estimate?**

Optical flow produces a spatially varying motion field — different parts of the image can have different velocities. This captures rotation, divergence, and shear within storm systems. Cross-correlation produces a single bulk motion vector. Tracking-based motion requires cells to already be linked, creating a circular dependency with the tracking module. Optical flow is the right tool for this role.

**Why store projections as a (step, y, x) label array rather than as centroid coordinates?**

Label arrays preserve spatial shape information. The analysis module can compute projected centroids for any cell by applying a centroid operation to each step. Storing only centroids would lose the shape, making it impossible to assess cell overlap or change in area across projection steps.
