# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Third-party libraries must not splatter their own banners to the Adapt console.

Py-ART prints a citation block on import; the ingest module imports pyart at its own
import time (pulled in via the runtime import chain) — *before* any Adapt module could
set PYART_QUIET. So the suppression must live in the package root (adapt/__init__),
which runs before every submodule. Verified in clean subprocesses importing the
runtime path that triggers the pyart import.
"""

import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit


def _import_clean(statement: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "PYART_QUIET"}
    return subprocess.run(
        [sys.executable, "-c", statement],
        capture_output=True,
        text=True,
        env=env,
    )


def test_importing_runtime_does_not_print_pyart_citation():
    # adapt.runtime pulls in pyart (via the ingest import chain); the banner must
    # still be suppressed.
    result = _import_clean("import adapt.runtime.processor")
    assert result.returncode == 0, result.stderr
    assert "Py-ART" not in result.stdout
    assert "jors.119" not in result.stdout


def test_importing_top_level_adapt_sets_pyart_quiet():
    result = _import_clean("import adapt, os; print(os.environ.get('PYART_QUIET'))")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] != "None"
