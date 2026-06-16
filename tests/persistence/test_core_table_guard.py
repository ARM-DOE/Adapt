# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Post-processors may only create extension tables, never mutate core tables.

The guard lives at the persistence boundary so every writer (live phase-3 enrich
and the PostProcessor) is bound by it.
"""

import pytest

from adapt.contracts import SqliteTable
from adapt.persistence.module_output import ModuleOutputWriter
from adapt.persistence.tables import CORE_TABLES, is_core_table

pytestmark = pytest.mark.unit


def test_core_tables_include_known_core_names():
    for name in ("cells_by_scan", "cell_events", "cell_tracks", "scans", "items"):
        assert name in CORE_TABLES
        assert is_core_table(name) is True


def test_extension_name_is_not_core():
    assert is_core_table("lma_cell_stats") is False


def test_writer_rejects_core_table(tmp_path):
    spec = SqliteTable(
        key="rows", table="cells_by_scan", primary_key=("run_id", "scan_time", "cell_uid")
    )
    with pytest.raises(ValueError, match="core table"):
        ModuleOutputWriter(tmp_path / "catalog.db", spec)


def test_writer_accepts_extension_table(tmp_path):
    spec = SqliteTable(key="rows", table="lma_cell_stats", primary_key=("cell_uid", "time_bin"))
    writer = ModuleOutputWriter(tmp_path / "catalog.db", spec)

    assert writer is not None
