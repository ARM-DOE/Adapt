# Configuration System

**Location**: `src/adapt/configuration/`

---

## Responsibility

The configuration system resolves settings from three sources — expert scientific defaults, user overrides, and command-line flags — into a single, frozen, fully validated configuration object that the rest of the system uses without further resolution logic.

Once `InternalConfig` is produced, configuration is done. No component reads YAML files, inspects environment variables, or applies default values at runtime.

---

## What It Is Not Responsible For

- Initialising runtime resources (directories, databases) — that is `init_runtime_config`'s job
- Validating scientific correctness of parameter values — it validates types and ranges, not physical sense
- Persisting user configuration files — the user is responsible for their config.yaml
- Providing dynamic configuration that can change at runtime — the frozen config is immutable

---

## The Three-Tier Hierarchy

```
CLI flags          ← Highest precedence (direct user intent)
    ↓ overrides
User config.yaml   ← User overrides of expert defaults
    ↓ overrides
defaults.yaml      ← Expert scientific defaults (ParamConfig)
    ↓
InternalConfig     ← Single frozen result
```

A value set via CLI flag always wins. A value set in the user's config.yaml wins over the expert default. If neither CLI nor user sets a value, the expert default from `defaults.yaml` is used.

This means a user can share a config.yaml that contains only the fields they actually care about (e.g., `radar: KDIX`, `mode: realtime`). Everything else comes from the expert defaults.

---

## Components

### ParamConfig (`schemas/param.py`)

The **expert default configuration**. Every tunable parameter in the system has a value here. This is the canonical scientific baseline — the values an expert would choose if the user specified nothing.

Sections:
```
downloader:
    radar              # NEXRAD site code
    mode               # realtime | historical
    poll_interval      # seconds between S3 polls (realtime)
    latest_files       # number of files to keep in rolling window
    latest_minutes     # time window for rolling window

regridder:
    grid_shape         # (nz, ny, nx) — number of grid points
    grid_limits        # spatial extent (km) from radar origin
    roi_func           # radius-of-influence function
    min_radius         # minimum interpolation radius (km)
    weighting_function # Cressman | Barnes

segmenter:
    threshold          # minimum reflectivity (dBZ)
    closing_kernel     # morphological closing radius (pixels)
    min_cellsize       # minimum cell area (grid points)
    max_cellsize       # maximum cell area (grid points)

analyzer:
    radar_variables    # list of fields to compute statistics on
    exclude_fields     # fields always excluded from statistics

projector:
    method             # optical flow algorithm
    max_projection_steps  # forecast steps (capped at 10)
    max_time_interval_minutes  # maximum gap between frames
    flow_params        # algorithm-specific parameters

global_:
    var_names          # canonical field name mappings

visualization:
    dpi, figsize, basemap, colors, ...

logging:
    level              # DEBUG | INFO | WARNING
```

### UserConfig (`schemas/user.py`)

The same structure as `ParamConfig` but with all fields optional. The user's `config.yaml` is validated against this schema. Unknown fields raise a `ValidationError` immediately — no silent ignoring of typos.

### CLIConfig (`schemas/cli.py`)

Settings that can only be meaningfully set via the command line:
```
--radar            → radar_id
--mode             → mode
--start-time       → start_time
--end-time         → end_time
--base-dir         → base_dir
--rerun            → rerun (bool flag)
--no-plot          → no_plot (bool flag)
--verbose          → verbose (bool flag)
```

### InternalConfig (`schemas/internal.py`)

The final, frozen, fully resolved configuration. This is a Pydantic model with `frozen=True`. Once created, no component can modify it.

Additional fields not in user configs:
```
run_id             # Generated at init time (e.g., "20260226-1530-KDIX")
```

### resolve_config (`schemas/resolve.py`)

The **single authoritative merge function**. Takes `ParamConfig`, `UserConfig`, and `CLIConfig`, applies the three-tier precedence, and returns a merged dict ready for `InternalConfig` construction.

**Merge rules**:
- Nested dicts: recursively merged (user fills in only what it overrides)
- Lists: replaced entirely (a user list replaces the default list, not extends it)
- Scalar values: replaced (higher precedence wins)

**Special merge rules** (documented exceptions to the above):
- `analyzer.exclude_fields`: user list is **unioned** with default list, never replaces it. Rationale: the default exclusions exist for a reason (coordinate fields, internal labels); the user should add to them, not accidentally remove them.
- `projector.max_projection_steps`: capped at 10 regardless of user input. Rationale: beyond 10 steps, projection accuracy degrades below a physically meaningful threshold.
- Mode inference: if `start_time` or `end_time` is set via CLI, mode is automatically set to `historical` if not explicitly provided.

### init_runtime_config (`schemas/initialization.py`)

The **complete runtime setup entrypoint**. This is the only function the CLI calls to go from raw CLI arguments to a ready-to-use `InternalConfig`.

**Steps**:
1. Load user config from YAML or Python file (or use empty defaults if none provided)
2. Call `resolve_config(param, user, cli)` to merge
3. Create output directories on disk
4. Handle `--rerun`: delete existing radar output and reset file tracker
5. Generate or reuse `run_id`
6. Open the store and begin the run (config recorded in registry.runs.config_json)
7. Persist the final resolved config to `{run_dir}/config.json` (for reproducibility audit)
8. Return `InternalConfig`

After this function returns, the system is fully initialised. No component downstream does any further resource setup.

---

## Configuration Persistence

The final resolved config is written to `{run_dir}/config.json` at startup. This means:
- Every pipeline run has a complete record of the exact settings used
- You can reproduce any historical run exactly by pointing to its config.json
- Debugging anomalous outputs always starts with "what were the parameters?"

---

## Design Notes

**Why freeze InternalConfig?**

A mutable configuration object in a multi-threaded system is a race condition waiting to happen. Immutability makes it safe to pass the config object to every thread without synchronisation. It also makes the system easier to reason about: parameters do not change during a run.

**Why validate user config strictly (no unknown fields)?**

Silent acceptance of unknown fields masks typos. If a user writes `threshhold: 30` (double h), strict validation raises an error immediately at startup rather than silently ignoring the override and running with the expert default all day.

**Why are expert defaults in a YAML file rather than hardcoded in Python?**

YAML is human-readable, version-controlled, and editable without touching Python. Scientists can propose a new default by editing `defaults.yaml` and opening a pull request. They do not need to navigate the Python schema code.
