# Adapt Dashboard — Reference

---

## Getting started (first-time user)

### 1. Open the dashboard

```bash
adapt dashboard
```

A welcome screen appears with two options.

---

### 2a. You already have data — open an existing repository

Click **Open an existing repository**, browse to the folder that contains
`adapt_registry.db` (the pipeline output directory), and click **Open Repository**.

The radar map and scan dropdowns populate automatically. Use the scan selector or
**Show Latest** to display a scan. Click any cell on the map to plot its track
history in the time-series panels on the right.

---

### 2b. You are starting fresh — run a new pipeline

Click **Start New Pipeline…** (or Pipeline → Start New… from the menu).

The launch wizard opens:

#### If you already have a `config.yaml`

1. Select **I have config file**.
2. Click **Browse…** and choose your `some_config.yaml`.
3. Set **Radar ID** (e.g. `KLOT`) and **Mode** (Realtime or Historical).
4. Click **Launch Pipeline**.

#### If you need to create a config

1. Select **Create config in directory**.
2. Click **Browse…** and choose the output directory (this becomes the
   repository root and the `base_dir` in the config).
3. Click **Create Config** — Adapt runs `adapt config` and writes a
   `config.yaml` template into that directory if config.yaml already exist, it shows message that config.yaml exists. IF you want new config.yaml rename the old yaml or delet it.
4. when config.yaml is create, a message appears: *"config.yaml created … Check config before running
   or click Launch Pipeline."*
5. Open the file in any text editor and check all defaults.
6. When ready, click **Launch Pipeline** in the wizard (it stays open).
   You can also skip editing and click **Launch Pipeline** immediately to
   run with the generated defaults.

---

### 3. While the pipeline is running

- The **● Pipeline running** badge appears in the toolbar.
- Switch to the **Log** tab to see live pipeline output.
- The radar map updates automatically as new scans arrive (checks every 10 s).
- The repository is loaded automatically — the radar and run dropdowns
  populate within a few seconds of the first analysis being written.

---

### 4. Exploring data

| Action | Result |
|--------|--------|
| Click a cell on the radar map | Plots that cell's full track history in the three time-series panels |
| Click again | Deselects the cell |
| Up to 7 cells | Each gets a distinct colour; the track legend shows at the bottom right |
| **Show Latest** | Jumps to the most recent scan; preserves your zoom and selected cells |
| **Show Loop** | Animates the last N scans; time-series panels update with each frame, vertical line marks the current scan time |
| Left / Right arrow keys | Step through scans one at a time |
| Space | Jump to latest scan |
| Scroll to zoom, drag to pan | Zoom is preserved across scan changes |

---

### 5. Stopping the pipeline

Click **Pipeline → Stop** in the menu or **■ Stop Pipeline** in the Log tab.
The badge returns to **○ Idle**.

If you close the dashboard while the pipeline is running, a dialog asks whether
to stop it. The pipeline can also keep running independently — reopen the
dashboard and it will offer to reconnect to the running process.

---

---

## Repository layout (pipeline writes this, dashboard reads it)

```
{repo}/
├── adapt_registry.db          # SQLite: run registry (radars + run metadata)
├── adapt_registry.db-shm      # SQLite WAL shared-memory file
├── adapt_registry.db-wal      # SQLite WAL log
└── {RADAR_ID}/
    ├── catalog.db             # SQLite: per-radar item registry (WAL mode)
    └── analysis/
        ├── {date}/
        │   └── *_analysis.nc  # NetCDF scan files (one per scan)
        └── analysis2d_*.parquet  # Cell statistics (parquet, one per run)
```

The dashboard treats this entire tree as read-only. It reads `adapt_registry.db`
to populate the radar and run dropdowns, reads `catalog.db` via `TrackStore` to
load track histories, and opens the `.nc` files directly with xarray for
rendering.

**The dashboard will not touch this directory unless a pipeline is launched from
the wizard.** Even then, the pipeline process is responsible for all writes — the
dashboard only redirects the pipeline's stdout/stderr to `~/.adapt/pipeline.log`.

---

## The `~/.adapt/` directory (dashboard writes here)

All dashboard-owned state lives under `~/.adapt/`:

```
~/.adapt/
├── pipeline.pid        # Written by the pipeline process, deleted on clean exit
├── pipeline.log        # Pipeline stdout+stderr; overwritten on each new launch
└── user_dashboard.json # Dashboard preferences (see below)
```

### `pipeline.pid`

Written by `adapt run-nexrad` at startup via `_write_pid()` in `cli.py`.
Deleted by the pipeline on clean exit (`atexit` / SIGTERM handler).

The dashboard uses this file to detect whether a pipeline is alive without owning
its `subprocess.Popen` handle — for example, after a GUI restart. The check is:

```
file exists  →  read PID  →  os.kill(pid, 0)  →  ProcessLookupError = dead
```

If `os.kill` raises `ProcessLookupError` the dashboard deletes the stale file and
reports idle. If it raises `PermissionError` the process exists but is owned by a
different user — reported as running.

### `pipeline.log`

Created (or overwritten) by the dashboard when it launches a pipeline. The file
is opened in line-buffered mode and passed directly as the subprocess `stdout`
handle, so all pipeline output (including stderr, which is redirected to stdout)
lands here in real time.

A daemon thread (`LogTail`) tails this file while the pipeline is running.
On reconnect after a GUI restart the last 200 lines are replayed into the Log tab.
The log is not rotated — it is always overwritten at the start of each new run.

### `user_dashboard.json`

Single JSON file that stores two independent sections:

```json
{
  "recent_repos": [
    "/path/to/most-recent-repo",
    "/path/to/older-repo"
  ],
  "configs": {
    "my_plot_setup": { ... },
    "another_preset": { ... }
  }
}
```

**`recent_repos`** — up to 5 most recently opened repository paths, most recent
first. Updated whenever the user opens a repository via File > Open or through
the wizard. Loaded on startup so the dashboard auto-connects to the last-used
repository without prompting.

**`configs`** — named snapshots of the plot configuration (see below). Saved via
Config > Save Config As…. Loaded via Config > Load Config.

---

## Two separate config concerns

These are completely different files with completely different purposes. Keep them
mentally separate.

### 1. Pipeline config — `config.yaml` (in the workspace)

Owned and interpreted by `adapt run-nexrad`. The dashboard **never creates or
modifies it directly**. Its location is inside the workspace (repository
directory), alongside the pipeline output.

```yaml
# Typical config.yaml keys
radar: KLOT
mode: realtime
base_dir: /data/radar_output
grid_shape: [41, 301, 301]
threshold: 40
...
```

To create a template: `adapt config /path/to/workspace/config.yaml`

The wizard checks whether `config.yaml` is present before launching. If it is
absent it runs `adapt config` to generate a template there, shows the path, and
**stops**. The user must open the file, set `radar` and `base_dir` at minimum,
then click Launch again. The dashboard never auto-launches with an unreviewed
config.

### 2. Dashboard visualization config (in `~/.adapt/user_dashboard.json`)

Owned entirely by the dashboard. Has nothing to do with the pipeline.

The bundled defaults live in the source tree at:

```
src/adapt/consumers/live/dashboard_default_config.json
```

This file is read at startup and never modified. It defines:

| Key | Purpose |
|-----|---------|
| `colors` | 7 hex colors for multi-cell track selection |
| `plot_groups` | Variable → axis mapping for the three time-series panels |
| `plot_assignments` | Which three groups are shown (`["Area", "Reflectivity", "ZDR"]`) |
| `overflow_action` | What to do when more than 7 cells are selected |

The `plot_groups` structure:

```json
"Area": {
  "variables": ["cell_area_sqkm", "area_40dbz_km2"],
  "styles":    ["solid",          "dashed"],
  "labels":    ["Cell area (km²)","Core area (km²)"]
}
```

The axis title is built as a compact single line:
`"Area   ─ Cell area (km²)   -- Core area (km²)"`

Named snapshots of this config can be saved via Config > Save Config As… and
are stored in `~/.adapt/user_dashboard.json` under the `configs` key. Loading
a snapshot replaces the in-memory state; the bundled defaults are unaffected.

**Hard-coded constants** (not in either config file):

| Setting | Value | Location |
|---------|-------|----------|
| Auto-refresh interval | 10 s | `POLL_MS` in `dashboard.py` |
| Log buffer (max lines) | 500 | `LOG_MAX` in `dashboard.py` |
| PID file path | `~/.adapt/pipeline.pid` | `_utils.py` |
| Log file path | `~/.adapt/pipeline.log` | `dashboard.py` |
| User config path | `~/.adapt/user_dashboard.json` | `_config.py` |
| Max recent repos | 5 | `_config.py` |

---

## Pipeline health monitoring

The dashboard tracks pipeline state through three independent mechanisms, active
simultaneously:

### 1. `subprocess.Popen` handle (`self._proc`)

When the dashboard launches the pipeline itself it holds a `Popen` object.
A dedicated daemon thread (`ProcWatcher`) calls `proc.wait()`, which blocks
until the process exits. When it unblocks it fires `_on_proc_ended()` on the
main thread via `self.after(0, ...)`. This gives near-instant death detection
(typically < 1 s) with no polling.

### 2. PID file polling (external / reconnect case)

When the dashboard restarts and finds `pipeline.pid` pointing to a live process
it has no `Popen` handle. After the user accepts the reconnect offer the dashboard
polls `_pipeline_running()` every 2 s via `_poll_external_pid()`. This is slower
than the watcher thread but sufficient for the reconnect scenario.

### 3. `_pipeline_running()` guard

Every auto-refresh cycle (`_refresh_all`, every 10 s) calls `_pipeline_running()`
to update the status bar and badge. This acts as a fallback for any state that
slipped through mechanisms 1 or 2.

### Badge states

| Badge text | Colour | Meaning |
|-----------|--------|---------|
| `● Pipeline running` | Green | `Popen` alive or PID file points to live process |
| `○ Idle` | Gray | No live process found |

### What happens on clean pipeline exit

1. Pipeline deletes `pipeline.pid` and exits (return code 0)
2. `ProcWatcher` thread unblocks from `proc.wait()`
3. Log file handle is closed
4. `_on_proc_ended()` is called on the main thread
5. Badge → `○ Idle`, status bar shows exit code

### What happens on crash / SIGKILL

1. Process dies; `pipeline.pid` is not deleted (atexit handler did not run)
2. `ProcWatcher` unblocks from `proc.wait()` (same as clean exit)
3. `_on_proc_ended()` fires; badge → `○ Idle`
4. Stale `pipeline.pid` remains; next `_pipeline_running()` call detects
   `ProcessLookupError` and deletes it automatically

---

## Auto-refresh and data loading

The dashboard auto-refreshes every 10 s (`POLL_MS`). Each cycle:

1. Calls `_get_nc_files(repo, radar)` — filesystem glob for `*_analysis.nc`
2. Updates the scan selector dropdown
3. If a new NC file appeared since the last render → re-renders in place
   (reuses the existing canvas, preserves zoom and cell selection)
4. Updates the status bar: scan count, pipeline state
5. Flushes the Log tab if it is visible

Data is never pre-fetched. Each scan is opened with `xr.open_dataset()` on
demand. Cell statistics are loaded once per run via `_load_cells_data()` and
cached in `self._current_cell_df`.

---

## Module layout

```
src/adapt/consumers/live/
├── dashboard.py                  # Main Tk window; all UI logic
├── _config.py                    # ~/.adapt/user_dashboard.json I/O only
├── _utils.py                     # Pure helpers; PID file logic; no Tk
├── _widgets.py                   # Custom Tk widgets (_CompactToolbar, _RangeSlider)
├── _timeseries.py                # matplotlib time-series helpers; no Tk
├── _renderer.py                  # RenderConfig dataclass; add_basemap()
└── dashboard_default_config.json # Bundled default plot configuration
```

`dashboard.py` is the only file that imports Tk. All other modules are pure
functions or dataclasses and are unit-tested without a display.
