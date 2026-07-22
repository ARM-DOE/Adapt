# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Importing the ingest module must not splash the Py-ART citation banner to stdout.

Py-ART prints a multi-line citation block on import unless ``PYART_QUIET`` is set.
That block is pure console clutter for an Adapt run. The ingest module owns the only
``import pyart`` in the package, so it must set the supported suppression env var
*before* importing pyart. Verified in a clean subprocess so the assertion reflects a
fresh interpreter, not one where pyart was already imported by the test session.
"""

import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit


def test_importing_ingest_does_not_print_pyart_citation():
    env = {k: v for k, v in os.environ.items() if k != "PYART_QUIET"}
    result = subprocess.run(
        [sys.executable, "-c", "import adapt.modules.ingest.module"],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert "Py-ART" not in result.stdout
    assert "jors.119" not in result.stdout
