# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Headless-import safety for the live consumer package.

CI runs on a headless runner with no Tk display. Importing a Tk-free submodule of
``adapt.consumers.live`` (for its pure logic or its Agg-only renderer) must not
drag in the GUI ``dashboard`` module, which selects the interactive ``TkAgg``
backend at import time and raises ``ImportError`` when no display exists.

Each import runs in a fresh interpreter so ``sys.modules`` is clean, making the
invariant deterministic on any platform — including developer machines that do
have Tk, where the eager import would otherwise succeed and hide the defect.
"""

import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit

_PURE_MODULES = [
    "adapt.consumers.live",
    "adapt.consumers.live._view_mode",
    "adapt.consumers.live._timers",
    "adapt.consumers.live._renderer",
]


@pytest.mark.parametrize("module", _PURE_MODULES)
def test_importing_pure_module_does_not_load_gui_dashboard(module: str) -> None:
    """A Tk-free live import must not transitively import the Tk dashboard."""
    code = (
        f"import sys, {module}; "
        "sys.exit(1 if 'adapt.consumers.live.dashboard' in sys.modules else 0)"
    )
    result = subprocess.run(  # noqa: S603 — fixed args, same interpreter
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"importing {module!r} pulled in adapt.consumers.live.dashboard, which selects "
        "the interactive TkAgg backend at import and cannot load on a headless CI "
        f"runner.\nstderr:\n{result.stderr}"
    )
