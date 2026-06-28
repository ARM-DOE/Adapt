# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""GraphExecutor auto-instruments every node with a span — zero module code.

Behaviour under test: one span per node, real durations from the injected clock,
the module runs inside its own stage context, and a node failure is recorded on
the span and re-raised (never swallowed). A graph with no provider still runs.
"""

import random
from datetime import UTC, datetime

import pytest

from adapt.execution.graph.builder import GraphBuilder
from adapt.execution.graph.executor import GraphExecutor
from adapt.modules.base import BaseModule
from adapt.runtime.observability import ObsSettings, build_observability


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


def _obs(**settings):
    return build_observability(
        ObsSettings(**settings),
        clock=lambda: 0.0,
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def _obs_seq_clock(values):
    it = iter(values)
    return build_observability(
        ObsSettings(),
        clock=lambda: next(it),
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def test_executor_opens_one_span_per_node_with_real_durations() -> None:
    obs = _obs_seq_clock([0.0, 1.0, 1.0, 3.0])  # ingest enter/exit, detection enter/exit
    ingest = _Stub("ingest", [], ["g"])
    detection = _Stub("detection", ["g"], ["c"])
    GraphExecutor(GraphBuilder([detection, ingest]).build(), observability=obs).run({})
    spans = {s.name: s for s in obs.drain_spans()}
    assert set(spans) == {"ingest", "detection"}
    assert spans["ingest"].duration_s == 1.0
    assert spans["detection"].duration_s == 2.0


def test_node_runs_inside_its_own_stage_context() -> None:
    obs = _obs()
    seen: dict[str, str] = {}

    def fn(ctx):
        seen["stage"] = obs.current().stage
        return {"g": 1}

    GraphExecutor(GraphBuilder([_Stub("ingest", [], ["g"], fn)]).build(), observability=obs).run({})
    assert seen["stage"] == "ingest"


def test_node_failure_records_error_on_span_and_propagates() -> None:
    obs = _obs()

    def boom(ctx):
        raise ValueError("kaboom")

    nodes = GraphBuilder([_Stub("detection", [], ["c"], boom)]).build()
    with pytest.raises(ValueError, match="kaboom"):
        GraphExecutor(nodes, observability=obs).run({})
    span = obs.drain_spans()[0]
    assert span.name == "detection"
    assert "ValueError: kaboom" in span.error
    assert obs.metrics.counter_total("errors_total") == 1.0


def test_node_failure_annotates_failing_stage() -> None:
    """A propagating module exception must name the failing stage.

    The scan-level handler logs one failure line; without this annotation it cannot
    say *which* stage broke. Works with or without a provider (it identifies the
    stage, independent of telemetry), so assert it on the no-provider path too.
    """

    def boom(ctx):
        raise ValueError("kaboom")

    nodes = GraphBuilder([_Stub("detection", [], ["c"], boom)]).build()
    with pytest.raises(ValueError) as excinfo:
        GraphExecutor(nodes).run({})

    notes = getattr(excinfo.value, "__notes__", [])
    assert any("detection" in note for note in notes)


def test_graph_runs_without_a_provider() -> None:
    nodes = GraphBuilder([_Stub("ingest", [], ["g"])]).build()
    result = GraphExecutor(nodes).run({})  # no observability -> no telemetry, still runs
    assert result["g"] == 1
