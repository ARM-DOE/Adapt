# User Guide

This guide covers running the Adapt pipeline, using the dashboard, understanding
outputs, and configuring the system. For installation see [Installation](installation.md).

---

## Quick start

Make a directory, initialize it as a data store, and work inside it. `adapt init`
creates the store layout (nothing else ever does — the pipeline refuses to run in
an uninitialized directory). `adapt config` writes `config.yaml` there with
`base_dir` pointing at itself, and every other command reads it from the working
directory, so there are no paths to pass:

```bash
mkdir my_case && cd my_case
adapt init            # creates registry.db, logs/, collections/
adapt config          # skip if you already have a config.yaml here
```

Then, in **two terminals** with the conda environment active and both in that
directory:

**Terminal 1 — start the real-time pipeline:**

```bash
adapt run-nexrad --radar KLOT
```
Replace `KLOT` with any 4-letter NEXRAD site code (e.g. `KDIX`, `KFTG`, `KAMX`).
The pipeline runs until you press `Ctrl-C`.

**Terminal 2 — open the dashboard:**

```bash
adapt dashboard
```

Click **Show Latest** in the dashboard to see the most recent processed scan.
Press `Ctrl-C` in Terminal 1 to stop the pipeline.

---

### Historical mode

Process a fixed time window from the archive:

```bash
adapt run-nexrad --radar KLOT \
    --start-time 2025-03-05T18:00:00 \
    --end-time   2025-03-05T20:00:00
```

If `--start-time` or `--end-time` is provided, historical mode is selected
automatically — you do not need `--mode historical`.

### Using a configuration from elsewhere

A `config.yaml` in the working directory is picked up automatically. A config
kept anywhere else, or under another name, is passed as the first argument:

```bash
adapt config my_config.yaml        # generate a template with all options
adapt run-nexrad my_config.yaml --radar KLOT
```

### Verbose logging

Add `-v` to see debug-level output, including per-scan timing and any errors:

```bash
adapt run-nexrad --radar KLOT -v
```

### All options

Each command lists its own flags:

```bash
adapt --help
adapt run-nexrad --help
adapt config --help
adapt dashboard --help
```

---

## Dashboard

Launch in a second terminal while the pipeline is running:

```bash
adapt dashboard
```

With no arguments it opens the working directory's repository if it is one, and
otherwise the repository you used last. Either way the toolbar's repository
selector switches to any other, so `--repo` is never required — it just
pre-populates that field:

```bash
adapt dashboard --repo /data/radar
```

A directory counts as a repository once `adapt init` has created `registry.db`
in it. Start the dashboard before the first scan is processed and the timeline
is simply empty until data arrives. Pre-store repositories (marked by the old
`adapt_registry.db`) are not readable — recreate them with `adapt init` and a
rerun.

The dashboard is **read-only** — it does not affect the pipeline.

### Controls

| Control | Description |
|---------|-------------|
| **Show Latest** | Jump to the most recent processed scan |
| **◄ / ►** | Step backward or forward one scan at a time |
| **Show Loop** | Animate the last N scans; set N and frame interval (ms) |
| **Variable** | Switch displayed field: reflectivity, ZDR, velocity, spectrum width |
| **Min / Max** | Set the colour-scale range; values outside are masked |
| **Proj steps** | Number of projected future positions to overlay (0 = show all) |
| **Hover** | Mouse over any cell to see its statistics in the side panel |

See [Dashboard Reference](dashboard_reference.md) for the full controls, launch wizard, and internals guide.

### Basemap

A background map overlay loads automatically if `contextily` is installed
(`pip install "arm-adapt[maps]"`). The first load fetches tiles from the
internet and may take a few seconds.

---

## Outputs — the data store

All pipeline artifacts live in the data store created by `adapt init` — the
repository root, which is the working directory unless the config or
`--base-dir` says otherwise:

```
my_case/
├── registry.db                # runs (with their full config), collections, events
├── logs/                      # run log files
└── collections/
    └── KLOT/                  # one collection per radar
        ├── catalog.db         # scans, artifacts, checksums, lineage
        ├── products.db        # cell stats, tracks, module tables, annotations
        └── objects/           # immutable NetCDF files, content-addressed
```

Files in `objects/` are named by artifact ID, not by scan time — everything is
discovered through the catalog, never by walking directories. Each scan is
identified by `scan_id`, the first 16 hex digits of the SHA-256 of the raw
Level-II file (verify externally with `sha256sum <raw file> | cut -c1-16`).
Every derived file records its checksum and its lineage back to that raw
volume, and every run records the exact configuration it ran with.

### Scan artifacts

Each scan produces a gridded 3-D NetCDF and a 2-D segmentation NetCDF
containing regridded radar fields, the cell label mask, and projected future
cell positions (optical flow). A scan is listed as complete only once all of
its required products exist, so readers never see half-written scans.

### Querying — StoreClient

Everything is queryable through the read-only [StoreClient API](api/client.rst):

```python
from adapt.api import StoreClient

client = StoreClient(".")  # or any store path
run = client.latest_run("KLOT")

cells = client.cells(run.run_id, "KLOT")            # all tracked cells
history = client.track_history(run.run_id, cells.iloc[0]["cell_uid"], "KLOT")

# Any module's table, with typed filters
big = client.table("cell_tracks", "KLOT", run_id=run.run_id,
                   filters={"max_reflectivity": {"op": "gt", "value": 50.0}})

# Arbitrary read-only SQL over products.db (catalog attached as `catalog`)
df = client.sql("SELECT COUNT(DISTINCT cell_uid) AS n FROM cells_by_scan", "KLOT")

client.close()
```

---

## Troubleshooting

### No data in dashboard after starting

The first scan takes longer (regridding + initial cell detection). Wait
30–60 seconds then click **Show Latest**.

### `No Adapt store at ...: registry.db not found`

Run `adapt init` in the directory first (the launch wizard in the dashboard
does this for you when starting a new pipeline).

### `... uses the obsolete pre-store layout (adapt_registry.db)`

The directory holds output from a pre-store version of Adapt. Move the old
data aside, run `adapt init`, and reprocess.

### Basemap not loading

`contextily` requires internet access to fetch map tiles. The first load for a
new area is slow. Check your network connection. Install if missing:

```bash
pip install "arm-adapt[maps]"
```

### Pipeline error on first scan

Re-run with `-v` to see the full traceback:

```bash
adapt run-nexrad --radar KLOT -v
```

### `adapt: command not found`

Activate the conda environment:

```bash
mamba activate adapt_env
```
