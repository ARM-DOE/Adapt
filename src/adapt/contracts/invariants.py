"""Formal pipeline invariants.

This file documents what each stage MUST produce. This is architecture, not code.
Use this file as a reviewer anchor and system reference.
"""

PIPELINE_INVARIANTS = {
    "grid": [
        "Dataset has 'x' and 'y' coordinates (Cartesian grid)",
        "Dataset has reflectivity variable (configured name)",
        "Coordinates and variables are 2D (already sliced at z-level)",
    ],

    "segmentation": [
        "Cell labels variable exists (configured name)",
        "Cell labels is integer typed (int32 or int64)",
        "Cell labels range: 0=background, 1..N=cells",
        "Largest cells are numbered first (decreasing size order)",
    ],

    "projection": [
        "When 2+ frames available: heading_x and heading_y exist",
        "Flow vectors are float typed (displacement per pixel)",
        "Optional cell_projections: projected labels for future steps",
        "Projection steps == configured max + 1 (1 registration + N projections)",
    ],

    "analysis": [
        "Output is DataFrame or dict-like with one row per cell",
        "cell_label column exists (matches segmentation IDs)",
        "Time columns are timezone-aware (UTC)",
        "All numeric fields are finite (no NaN in required columns)",
    ],

    "database": [
        "SQLite table 'cells' has schema matching analyzer output",
        "One row per cell per file (primary key on file_id + cell_label)",
        "Indexes exist on time columns for efficient querying",
    ],
}

# Which stages are optional vs required
STAGE_REQUIREMENTS = {
    "grid": "REQUIRED",      # Every file must be gridded
    "segmentation": "REQUIRED",  # Every file must be segmented
    "projection": "OPTIONAL",    # Only if 2+ frames
    "analysis": "REQUIRED",  # Every file must produce analysis
    "database": "REQUIRED",  # All analysis must be persisted
}
