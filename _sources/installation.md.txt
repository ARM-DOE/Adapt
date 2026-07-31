# Installation

## Requirements

- Python 3.10 or later
- Internet access (to download NEXRAD data from AWS S3)
- macOS, Linux, or Windows

---

## Recommended: conda-forge

Adapt is published on [conda-forge](https://anaconda.org/conda-forge/arm-adapt) for macOS, Linux, and
Windows. Installing from there keeps every compiled dependency (netCDF4, h5py, opencv, Py-ART) on
conda-native builds, which avoids the binary conflicts described below:

```bash
conda create -n adapt_env python=3.13 -y
conda activate adapt_env
conda install conda-forge::arm-adapt
```

Verify:

```bash
adapt --version
adapt --help
```

`adapt --version` prints the installed version and the path Adapt was installed to — useful for
confirming which environment is active.

---

## Alternative: pip

```bash
pip install arm-adapt
```

The environment must be **fresh** — do not `pip install` into a conda environment that already has
netCDF4, h5py, opencv, or Py-ART installed from conda-forge. Adapt's compiled dependencies ship both
conda and pip builds of the same native libraries, and mixing them fails at import with a DLL error —
on Windows, `[WinError 11] An attempt was made to load a program with an incorrect format`. On Windows,
use 64-bit x86 Python: several of these dependencies publish no ARM64 wheels. This is a pip/conda
mixing issue only — it does not apply to the conda-forge install above.

---

## Developers: latest source

To run the bleeding-edge version from `main` and have your edits take effect
immediately, install the checkout in editable mode:

```bash
conda create -n adapt_dev python=3.13 -y
conda activate adapt_dev
git clone https://github.com/ARM-DOE/Adapt.git
cd Adapt
pip install -e .
```

Then generate a config, run the pipeline, and open the dashboard:

```bash
adapt config                              # writes config.yaml in the current directory
adapt run-nexrad --radar KLOT             # Ctrl-C to stop
adapt dashboard                           # in a second terminal
```

`adapt config` with no argument writes `config.yaml` into the current directory
and sets `base_dir` to that same directory, so the three commands above work
together with no paths to pass.

> Use a fresh environment for the editable install too — the same pip/conda
> binary-mixing issue described above applies.

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
