# User Guide

This guide covers running the Adapt pipeline, using the dashboard, understanding
outputs, and configuring the system. For installation see [Installation](installation.md).

---

## Quick start

Open **two terminals**, both with the conda environment active.

**Terminal 1 — start the pipeline:**

```bash
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output
```

**Terminal 2 — open the dashboard:**

```bash
adapt dashboard --repo ~/adapt_output
```

Click **Show Latest** in the dashboard to see the most recent processed scan.
Press `Ctrl-C` in Terminal 1 to stop the pipeline.

---

## Running the pipeline

### Real-time mode

Continuously downloads and processes new scans as they are released:

```bash
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output
```

Replace `KLOT` with any 4-letter NEXRAD site code (e.g. `KDIX`, `KFTG`, `KAMX`).
The pipeline runs until you press `Ctrl-C`.

### Historical mode

Process a fixed time window from the archive:

```bash
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output \
    --start-time 2025-03-05T18:00:00 \
    --end-time   2025-03-05T20:00:00
```

If `--start-time` or `--end-time` is provided, historical mode is selected
automatically — you do not need `--mode historical`.

### Using a custom configuration

Generate a template, edit it, then pass it as the first argument:

```bash
adapt config my_config.yaml        # generate template with all options
adapt run-nexrad my_config.yaml --radar KLOT --base-dir ~/adapt_output
```

### Verbose logging

Add `-v` to see debug-level output, including per-scan timing and any errors:

```bash
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output -v
```

---

## Configuration

Running without a config file uses built-in defaults. Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `regridder.grid_shape` | `[41, 201, 201]` | Grid points (nz, ny, nx) |
| `regridder.grid_limits` | `±100 km` | Horizontal extent from radar |
| `segmenter.threshold` | `30 dBZ` | Reflectivity threshold for cell detection |
| `segmenter.min_cellsize_gridpoint` | `5` | Minimum cell size in grid points |

Generate a fully documented template with all available options:

```bash
adapt config my_config.yaml
```

---

## Dashboard

Launch in a second terminal while the pipeline is running:

```bash
adapt dashboard --repo ~/adapt_output
```

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

All pipeline artifacts are written under `--base-dir`:

```
~/adapt_output/
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

client = DataClient("~/adapt_output")
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
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output -v
```

### `adapt: command not found`

Activate the conda environment:

```bash
mamba activate adapt_env
```
