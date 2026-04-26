import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "Adapt"
author = "Bhupendra Raut"
copyright = "2026, Bhupendra Raut"
release = "0.2.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

myst_enable_extensions = ["colon_fence"]

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}
autodoc_typehints = "description"

html_theme = "sphinx_rtd_theme"
html_static_path = []

templates_path = []
exclude_patterns = ["_build", "design", "*.md.bak"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
