# User Guide

This guide covers running the Adapt pipeline, using the dashboard, understanding
outputs, and configuring the system. For installation see [Installation](installation.md).

---

## Quick start

Make a directory and work inside it — that directory becomes the repository.
`adapt config` writes `config.yaml` there with `base_dir` pointing at itself, and
every other command reads it from the working directory, so there are no paths to
pass:

```bash
mkdir my_case && cd my_case
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


```bash
adapt config my_config.yaml
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

A directory only counts as a repository once the pipeline has run in it and
created `adapt_registry.db`. Start the dashboard before that and it falls back
to your previous repository; use the selector, or just wait for the first scan.

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

### Basemap

A background map overlay loads automatically if `contextily` is installed
(`pip install "arm-adapt[maps]"`). The first load fetches tiles from the
internet and may take a few seconds.

---

## Outputs

All pipeline artifacts are written under `base_dir` — the repository root,
which is the working directory unless the config or `--base-dir` says otherwise:

```
my_case/
├── KLOT/
│   ├── nexrad/                        # raw Level-II files from AWS
│   ├── gridnc/
│   │   └── 20250305/
│   │       └── KLOT20250305_183210_V06.nc   # regridded Cartesian NetCDF
│   └── analysis/
│       ├── 20250305/
│       │   └── KLOT_20250305_183210_analysis.nc  # per-scan analysis
│       └── catalog.db                 # SQLite: cell records, tracking events
├── adapt_registry.db                  # run registry
└── runtime_config_<run-id>.json       # configuration snapshot for this run
```

### Analysis NetCDF

Each scan produces one NetCDF file containing:
- Regridded radar fields (reflectivity, ZDR, velocity, etc.)
- Cell label mask
- Projected future cell positions (optical flow)

### Catalog database

`catalog.db` is a SQLite database with WAL journalling. Query it directly or
use the [DataClient API](api/client.rst):

```python
from adapt.api import DataClient

client = DataClient(".")  # or any repository path
df = client.latest("cells_by_scan", radar="KLOT")
```

---

## Troubleshooting

### No data in dashboard after starting

The first scan takes longer (regridding + initial cell detection). Wait
30–60 seconds then click **Show Latest**.

### `No *_analysis.nc for today`

The pipeline has not produced output yet. Check the **Log** tab in the dashboard
for errors.

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
