# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""End-to-end: modules run through the real executor populate execution history.

Acceptance for the observability + execution-history chain — span seam -> drain
-> ExecutionHistory -> SQL query — with ZERO instrumentation code in the modules.
"""

import random
from datetime import UTC, datetime

import pytest

from adapt.contracts.execution_history import RunProvenance, RunStart
from adapt.execution.graph.builder import GraphBuilder
from adapt.execution.graph.executor import GraphExecutor
from adapt.modules.base import BaseModule
from adapt.persistence.execution_history import ExecutionHistory
from adapt.runtime.observability import ObsSettings, build_observability

pytestmark = pytest.mark.integration


class _Stub(BaseModule):
    def __init__(self, name, inputs, outputs, fn=None):
        self._name, self._inputs, self._outputs, self._fn = name, inputs, outputs, fn

    @property
    def name(self):
        return self._name

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def run(self, context):
        return self._fn(context) if self._fn else {k: 1 for k in self._outputs}


def _obs():
    return build_observability(
        ObsSettings(),
        clock=lambda: 0.0,
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def _started_history(tmp_path):
    history = ExecutionHistory(tmp_path / "catalog.db")
    history.start_run(
        RunStart(
            run_id="R1",
            pipeline="nexrad",
            pipeline_version="0.4.1",
            site="KDIX",
            dataset="KDIX",
            instrument="NEXRAD",
            mode="historical",
            start_time=datetime(2026, 6, 28, tzinfo=UTC),
            configuration_hash="abc",
            configuration_file="cfg.yaml",
            provenance=RunProvenance(None, "h", "u", "3.11", "linux", "0.4.1"),
            enabled_modules=("ingest", "detection"),
        )
    )
    return history


def test_modules_run_populate_module_history_and_share_one_trace(tmp_path):
    obs = _obs()
    history = _started_history(tmp_path)
    ingest = _Stub("ingest", [], ["g"])
    detection = _Stub("detection", ["g"], ["c"])

    with obs.span("scan"):
        GraphExecutor(GraphBuilder([detection, ingest]).build(), observability=obs).run({})

    module_spans = [s for s in obs.drain_spans() if s.name != "scan"]
    history.record_modules(
        "R1", "scan1", module_spans, recorded_at=datetime(2026, 6, 28, tzinfo=UTC)
    )

    rows = {m["module"]: m for m in history.query_modules(run_id="R1")}
    assert set(rows) == {"ingest", "detection"}
    assert all(r["status"] == "ok" for r in rows.values())
    assert len({s.trace_id for s in module_spans}) == 1  # one trace spans the scan


def test_failing_module_records_error_row_and_failure_rate(tmp_path):
    obs = _obs()
    history = _started_history(tmp_path)

    def boom(_ctx):
        raise ValueError("kaboom")

    nodes = GraphBuilder([_Stub("detection", [], ["c"], boom)]).build()
    with pytest.raises(ValueError, match="kaboom"):
        GraphExecutor(nodes, observability=obs).run({})

    module_spans = [s for s in obs.drain_spans() if s.name != "scan"]
    history.record_modules(
        "R1", "scan1", module_spans, recorded_at=datetime(2026, 6, 28, tzinfo=UTC)
    )

    assert history.query_modules(run_id="R1")[0]["status"] == "error"
    assert history.failure_rate_by_module()["detection"] == 1.0
