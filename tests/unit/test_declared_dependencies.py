# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Regression tests for dependency hygiene.

Bug history: duckdb was imported unconditionally but absent from
pyproject.toml (fresh installs crashed); later the store redesign removed the
DuckDB machinery entirely — a declared-but-unused dependency is the inverse
smell, so both directions are pinned.
"""

import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_ROOT = Path(__file__).parents[2]
_PYPROJECT = _ROOT / "pyproject.toml"


def _declared_deps() -> set[str]:
    with open(_PYPROJECT, "rb") as f:
        data = tomllib.load(f)
    return {
        d.split("[")[0].split(">=")[0].split("==")[0].strip().lower()
        for d in data["project"]["dependencies"]
    }


def test_duckdb_is_fully_removed():
    """The store redesign deleted the DuckDB escape hatch: it must be neither
    imported anywhere in src nor declared as a dependency."""
    assert "duckdb" not in _declared_deps()
    hits = [
        str(py)
        for py in (_ROOT / "src").rglob("*.py")
        if "duckdb" in py.read_text(encoding="utf-8")
    ]
    assert hits == []
