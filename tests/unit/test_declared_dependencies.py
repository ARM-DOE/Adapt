# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Dependency-declaration fitness tests: imports and pyproject must agree.

Bug history: duckdb was imported unconditionally but absent from
pyproject.toml, so fresh installs crashed; later cmweather and pyproj drifted
the same way. Instead of pinning individual packages, these tests enforce the
general rule in both directions:

- every third-party import in src/ maps to a declared dependency;
- every EAGER (module-level) import maps to a *hard* dependency — an
  optional extra is only acceptable for imports deferred into functions;
- every declared hard dependency is imported somewhere, unless it is on the
  shrink-only allowlist with a reason (e.g. engines loaded by entry point).
"""

import ast
import sys
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_ROOT = Path(__file__).parents[2]
_SRC = _ROOT / "src" / "adapt"
_PYPROJECT = _ROOT / "pyproject.toml"

# Import name -> distribution name, where the two differ. botocore maps to
# boto3 because boto3 hard-depends on it; declaring both would be redundant.
_IMPORT_TO_DIST = {
    "cv2": "opencv-python",
    "skimage": "scikit-image",
    "sklearn": "scikit-learn",
    "pyart": "arm-pyart",
    "yaml": "pyyaml",
    "botocore": "boto3",
}

# Declared hard dependencies that are legitimately never imported.
# Shrink-only: add an entry only with a reason the import cannot exist.
_DECLARED_NOT_IMPORTED = {
    "netcdf4": "xarray NetCDF backend, loaded via entry point, never imported",
}

_LOCAL = {"adapt"}


def _canon(dist: str) -> str:
    """Canonical distribution name: lowercase, '-' separators, no extras/pins."""
    bare = dist.split("[")[0].split(">=")[0].split("==")[0].split("<")[0].strip()
    return bare.lower().replace("_", "-")


def _dist_for(import_name: str) -> str:
    return _canon(_IMPORT_TO_DIST.get(import_name, import_name))


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _guards_import_error(node: ast.Try) -> bool:
    """True for the optional-import idiom: try/except ImportError."""
    caught = {
        name.id
        for handler in node.handlers
        for name in ast.walk(handler.type)
        if handler.type is not None and isinstance(name, ast.Name)
    }
    return bool(caught & {"ImportError", "ModuleNotFoundError"})


def _collect_imports(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Return (eager, lazy) top-level import names.

    Eager = unconditionally executed at module import time.
    Lazy = inside a function, under `if TYPE_CHECKING:`, or guarded by
    `try/except ImportError` (the sanctioned optional-dependency idiom).
    """
    eager: set[str] = set()
    lazy: set[str] = set()

    def visit(node: ast.AST, is_eager: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.Import):
                names = {alias.name.split(".")[0] for alias in child.names}
                (eager if is_eager else lazy).update(names)
            elif isinstance(child, ast.ImportFrom) and child.module and child.level == 0:
                (eager if is_eager else lazy).add(child.module.split(".")[0])
            elif (
                (isinstance(child, ast.If) and _is_type_checking(child.test))
                or isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda))
                or (isinstance(child, ast.Try) and _guards_import_error(child))
            ):
                visit(child, False)
            else:
                visit(child, is_eager)

    visit(tree, True)
    return eager, lazy


def _src_imports() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Third-party imports across src/adapt: import name -> files using it."""
    eager: dict[str, set[str]] = {}
    lazy: dict[str, set[str]] = {}
    for py_file in _SRC.rglob("*.py"):
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
        file_eager, file_lazy = _collect_imports(tree)
        rel = py_file.relative_to(_SRC).as_posix()
        for name in file_eager:
            if name not in sys.stdlib_module_names and name not in _LOCAL:
                eager.setdefault(name, set()).add(rel)
        for name in file_lazy:
            if name not in sys.stdlib_module_names and name not in _LOCAL:
                lazy.setdefault(name, set()).add(rel)
    return eager, lazy


def _declared() -> tuple[set[str], set[str]]:
    """Return (hard dependencies, extras) as canonical distribution names."""
    with open(_PYPROJECT, "rb") as f:
        data = tomllib.load(f)
    hard = {_canon(d) for d in data["project"]["dependencies"]}
    extras = {
        _canon(d)
        for group, deps in data["project"].get("optional-dependencies", {}).items()
        if group not in {"dev", "docs"}  # tooling, never imported by src
        for d in deps
    }
    return hard, extras


def test_every_import_is_declared() -> None:
    """Every third-party import in src/ maps to a declared dependency."""
    eager, lazy = _src_imports()
    hard, extras = _declared()
    declared = hard | extras

    undeclared = {
        name: sorted(files)
        for imports in (eager, lazy)
        for name, files in imports.items()
        if _dist_for(name) not in declared
    }
    assert not undeclared, (
        f"Imported in src/ but not declared in pyproject.toml: {undeclared}. "
        "Declare the dependency (and give it a home in _DEP_HOMES if heavy)."
    )


def test_eager_imports_are_hard_dependencies() -> None:
    """A module-level import may not live only in an optional extra —
    installing without the extra would crash at import time."""
    eager, _ = _src_imports()
    hard, _ = _declared()

    extra_only = {
        name: sorted(files) for name, files in eager.items() if _dist_for(name) not in hard
    }
    assert not extra_only, (
        f"Eagerly imported but not a hard dependency: {extra_only}. "
        "Promote it to [project].dependencies or defer the import into the "
        "function that needs it."
    )


def test_every_hard_dependency_is_imported() -> None:
    """Declared hard dependencies must be used, or allowlisted with a reason."""
    eager, lazy = _src_imports()
    hard, _ = _declared()
    used = {_dist_for(name) for name in (*eager, *lazy)}

    unused = hard - used - set(_DECLARED_NOT_IMPORTED)
    assert not unused, (
        f"Declared in pyproject.toml but never imported in src/: {sorted(unused)}. "
        "Remove the dependency, or add it to _DECLARED_NOT_IMPORTED with the "
        "reason no import can exist."
    )

    stale = set(_DECLARED_NOT_IMPORTED) - hard
    assert not stale, (
        f"_DECLARED_NOT_IMPORTED entries no longer declared: {sorted(stale)}. "
        "Remove them so the allowlist only shrinks."
    )
