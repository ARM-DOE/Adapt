# Installation

## Requirements

- Python 3.10 or later
- Internet access (to download NEXRAD data from AWS S3)
- macOS, Linux, or Windows

---

## Standard installation

```bash
pip install arm-adapt
```

Verify:

```bash
adapt --help
```

---

## Recommended: conda environment

Using conda or mamba avoids dependency conflicts, particularly with geospatial
libraries (PROJ, GDAL):

```bash
mamba create -n adapt_env python=3.13 -y
mamba activate adapt_env
pip install arm-adapt
adapt --help
```

---

## Optional: map overlay support

By default Adapt installs without `contextily` and `pyproj`. The dashboard runs
fully without them — radar data, cell tracking, and statistics all work. The
basemap overlay (background map tiles) and coordinate display in the toolbar
require the extra:

```bash
pip install "arm-adapt[maps]"
```

> **Note for conda users:** install `pyproj` via conda-forge instead of pip to
> avoid PROJ database version conflicts:
>
> ```bash
> pip install arm-adapt
> conda install -c conda-forge pyproj contextily
> ```

---

## Troubleshooting

### `adapt: command not found`

Activate the environment first:

```bash
mamba activate adapt_env
```

### PROJ database version errors

```
PROJ: proj.db contains DATABASE.LAYOUT.VERSION.MINOR = 4 whereas >= 6 is expected
```

pip-installed `pyproj` bundles an outdated PROJ database. Fix:

```bash
pip uninstall pyproj -y
conda install -c conda-forge pyproj
```

Or uninstall pyproj entirely — the dashboard works without it (basemap
and coordinate toolbar are disabled automatically).

### pyproj network warning

```
UserWarning: pyproj unable to set PROJ database path.
```

Harmless. Suppress with:

```bash
conda env config vars set PROJ_NETWORK=OFF -n adapt_env
```

### `ModuleNotFoundError` on dashboard startup

Make sure you installed from PyPI and not just cloned the repository:

```bash
pip install arm-adapt
```
