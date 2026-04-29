import os
import sys
import re
from importlib.metadata import version as _pkg_version

sys.path.insert(0, os.path.abspath("../src"))

project = "Adapt"
author = "Bhupendra Raut"
copyright = "2026, UChicago Argonne, LLC"
release = re.match(r"^\d+\.\d+\.\d+", _pkg_version("arm-adapt")).group(0)

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

autodoc_mock_imports = [
    "pyart", "arm_pyart",
    "cv2",
    "cartopy",
    "contextily",
    "nexradaws",
    "h5py",
    "netCDF4",
    "scipy",
    "skimage",
    "pyarrow",
    "pyproj",
]

myst_enable_extensions = ["colon_fence"]

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}
autodoc_typehints = "description"

root_doc = "readme"

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]

templates_path = []
exclude_patterns = ["_build", "design", "*.md.bak"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
