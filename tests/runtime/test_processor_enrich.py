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

from adapt.contracts import ProductTableWrite
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
        ProductTableWrite(
            key="enrich_probe_rows",
            table="enrich_probe",
            primary_key=("run_id", "scan_id", "cell_uid"),
            index_columns=("cell_uid",),
        ),
    )

    def run(self, context: dict) -> dict:
        # Identity columns are stamped by the store writer — never module-supplied.
        return {"enrich_probe_rows": pd.DataFrame([{"cell_uid": "a", "v": 1.0}])}


@pytest.fixture
def proc_with_enrich(pipeline_config, store_env):
    registry.register(_ProbeEnrichModule)
    try:
        proc = RadarProcessor(
            queue.Queue(),
            pipeline_config,
            collection=store_env.collection,
            registry=store_env.registry,
            run_id=store_env.run_id,
            history=store_env.history,
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
    def test_enrich_results_routed_to_declared_table(self, proc_with_enrich, store_env):
        from datetime import UTC, datetime

        from adapt.contracts import PersistenceMeta
        from adapt.contracts.persistence import ScanRecord

        store_env.collection.catalog.register_scan(
            ScanRecord(
                run_id=store_env.run_id,
                scan_id="sid-test",
                scan_time=datetime(2024, 1, 1, tzinfo=UTC),
                source_file_name="probe",
            )
        )
        ext_result = _ProbeEnrichModule().run({})
        meta = PersistenceMeta(
            scan_time=datetime(2024, 1, 1, tzinfo=UTC),
            scan_id="sid-test",
            run_id=store_env.run_id,
            source_file="",
            collection_id="TEST_RADAR",
        )
        proc_with_enrich._router.persist(proc_with_enrich._post_modules, ext_result, meta)

        conn = sqlite3.connect(str(store_env.collection.products_path))
        try:
            rows = conn.execute("SELECT run_id, scan_id, cell_uid, v FROM enrich_probe").fetchall()
        finally:
            conn.close()
        # The writer stamped run and scan identity from PersistenceMeta — enrich
        # modules never supply them.
        assert rows == [(store_env.run_id, "sid-test", "a", 1.0)]
