# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for RadarProcessor post-persistence (enrich) handling.

Enrich modules run only after cell_uid is committed, and their returned
DataFrame is written generically via the module's declared SqliteTable spec.
"""

import queue
import sqlite3

import pandas as pd
import pytest

from adapt.contracts import SqliteTable
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


class _ProbeEnrichModule(BaseModule):
    """Synthetic phase-3 module that emits a fixed DataFrame."""

    name = "enrich_probe"
    pipeline_phase = 3
    required_history = 1
    inputs = ["run_id", "scan_time"]
    outputs = ["enrich_probe_rows"]
    persistence = (
        SqliteTable(
            key="enrich_probe_rows",
            table="enrich_probe",
            primary_key=("run_id", "scan_time", "cell_uid"),
            index_columns=("scan_time", "cell_uid"),
        ),
    )

    def run(self, context: dict) -> dict:
        return {
            "enrich_probe_rows": pd.DataFrame(
                [{"run_id": "R1", "scan_time": "2024-01-01T00:00:00", "cell_uid": "a", "v": 1.0}]
            )
        }


@pytest.fixture
def proc_with_enrich(pipeline_config, pipeline_output_dirs, test_repository):
    registry.register(_ProbeEnrichModule)
    try:
        proc = RadarProcessor(
            queue.Queue(),
            pipeline_config,
            pipeline_output_dirs,
            repository=test_repository,
        )
        yield proc
    finally:
        registry.unregister("enrich_probe")


def _tracked(with_uid: bool) -> pd.DataFrame:
    cols: dict[str, list[object]] = {"cell_label": [1]}
    if with_uid:
        cols["cell_uid"] = ["a"]
    return pd.DataFrame(cols)


class TestEnrichUidGuard:
    def test_skips_enrich_when_tracked_cells_none(self, proc_with_enrich):
        assert proc_with_enrich._should_run_enrichment({"tracked_cells": None}) is False

    def test_skips_enrich_when_tracked_cells_empty(self, proc_with_enrich):
        assert proc_with_enrich._should_run_enrichment({"tracked_cells": pd.DataFrame()}) is False

    def test_skips_enrich_when_no_cell_uid_column(self, proc_with_enrich):
        result = {"tracked_cells": _tracked(with_uid=False)}
        assert proc_with_enrich._should_run_enrichment(result) is False

    def test_runs_enrich_when_cell_uid_present(self, proc_with_enrich):
        result = {"tracked_cells": _tracked(with_uid=True)}
        assert proc_with_enrich._should_run_enrichment(result) is True


class TestEnrichWrite:
    def test_enrich_results_routed_to_declared_table(self, proc_with_enrich, test_repository):
        from adapt.contracts import PersistenceMeta

        ext_result = _ProbeEnrichModule().run({})
        meta = PersistenceMeta(
            scan_time=None,
            run_id=test_repository.run_id,
            source_file="",
            dataset_id=test_repository.radar,
        )
        proc_with_enrich._router.persist(proc_with_enrich._post_modules, ext_result, meta)

        conn = sqlite3.connect(str(test_repository.catalog.db_path))
        try:
            rows = conn.execute("SELECT run_id, cell_uid, v FROM enrich_probe").fetchall()
        finally:
            conn.close()
        assert rows == [("R1", "a", 1.0)]
