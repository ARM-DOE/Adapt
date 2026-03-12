# Adapt

**Real-Time data processing for informed adaptive scanning of ARM weather radars**

Adapt downloads NEXRAD Level-II radar data from AWS, regrids to a Cartesian grid,
detects convective cells, projects cell motion, computes per-cell statistics, and
displays results in an interactive GUI dashboard — all in near real-time.

---

## Pipeline overview

```
AWS S3 (NEXRAD Level-II)
        │
        ▼
   Downloader
        │  raw files
        ▼
   Processor
        ├─ Load & Regrid    (Py-ART + xarray → Cartesian grid)
        ├─ Detect           (threshold + morphology → cell labels)
        ├─ Projection       (optical flow → future cell positions)
        └─ Analysis         (per-cell statistics → Parquet)
        │
        ▼
   Output repository  (NetCDF + Parquet + SQLite catalog)
        │
        ▼
   Dashboard  (read-only Tkinter GUI)
```

---

## Installation

**Requirements:** conda or mamba, internet access.

```bash
git clone https://github.com/ARM-DOE/Adapt.git
cd Adapt
mamba env create -f environment.yml   # installs all deps + adapt in editable mode
mamba activate adapt_env
```

Verify:

```bash
adapt --help
```

---

## Usage

### 1. (Optional) Generate a config file

```bash
adapt config my_config.yaml
```

Without a config file, built-in defaults are used. Key parameters in the YAML:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `regridder.grid_shape` | `[41, 201, 201]` | Grid points (nz, ny, nx) |
| `regridder.grid_limits` | `±100 km` | Horizontal extent from radar |
| `segmenter.threshold` | `30 dBZ` | Reflectivity threshold for cell detection |
| `segmenter.min_cellsize_gridpoint` | `5` | Minimum cell size (grid points) |

### 2. Run the pipeline

Real-time (continuous, press Ctrl-C to stop):

```bash
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output
```

Historical (specific time window):

```bash
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output \
    --mode historical \
    --start-time 2025-03-05T18:00:00 \
    --end-time   2025-03-05T20:00:00
```

With a custom config:

```bash
adapt run-nexrad my_config.yaml --radar KLOT --base-dir ~/adapt_output
```

Replace `KLOT` with any 4-letter NEXRAD site code (e.g. `KDIX`, `KFTG`, `KAMX`).
Add `-v` for verbose/debug logging.

### 3. Open the dashboard

In a **second terminal**:

```bash
mamba activate adapt_env
adapt dashboard --repo ~/adapt_output
```

Click **Show Latest** to display the most recent scan. Key controls:

| Control | Action |
|---------|--------|
| **Show Latest** | Display the most recent processed scan |
| **Show Loop** | Animate the last N scans (set N and dt ms) |
| **◄ / ►** | Step through scans one at a time |
| **Proj steps** | Projection steps to draw (0 = all available) |
| **Variable** | Switch between reflectivity, ZDR, velocity, spectrum width |
| **Min / Max** | Colour-scale range — values outside are masked |
| **Hover** | Move mouse over a cell to see its statistics |

---

## Output structure

```
~/adapt_output/
├── KLOT/
│   ├── nexrad/               # raw Level-II files
│   ├── grids/                # regridded NetCDF (3-D Cartesian)
│   └── analysis/
│       ├── 20250305/
│       │   └── KLOT_20250305_183210_analysis.nc
│       └── analysis2d_KLOT.parquet   # all-scan cell stats
├── adapt_registry.db
└── runtime_config_<id>.json
```

---

## Troubleshooting

**`adapt: command not found`** — run `mamba activate adapt_env` first.

**`No *_analysis.nc for today`** — pipeline has not produced output yet; check the Log tab.

**Basemap missing** — contextily needs internet for tile downloads; first load may be slow.

**Pipeline error on first scan** — re-run with `-v` to see the full traceback.

---

## Key features

- Real-time and historical processing modes
- Modular graph-based pipeline — modules declare inputs/outputs; execution order resolved automatically
- Configurable via YAML or CLI flags; CLI flags override config
- Interactive dashboard — pan/zoom, hover cell stats, loop animation, OpenStreetMap basemap, projection overlays
- Data API: `adapt.api.DataClient` to query results.
- Multiple downstream applications: analytical notebooks, adaptive scanning module, GUI

---

## Authors


## License
