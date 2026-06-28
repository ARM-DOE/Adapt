# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Executes a DAG of nodes in dependency order.

The executor runs a simple topological loop: nodes whose dependencies have
all completed are eligible to run. This single-threaded executor is the
reference implementation. Future steps may add parallel execution.

Contract enforcement
--------------------
If a module declares ``input_contracts`` or ``output_contracts``, the
executor calls each validator automatically before and after ``run()``.
Validators raise ``ContractViolation`` on failure; the exception propagates
up to the caller (e.g. ``RadarProcessor``) which handles it.
"""

import logging
from contextlib import nullcontext

from adapt.contracts.observability import Observability
from adapt.contracts.pipeline import require
from adapt.execution.graph.node import Node

logger = logging.getLogger(__name__)


class GraphExecutor:
    """Execute a list of nodes in topological order.

    Parameters
    ----------
    nodes : list[Node]
        Connected nodes returned by ``GraphBuilder.build()``.

    Example::

        nodes = GraphBuilder([a, b, c]).build()
        executor = GraphExecutor(nodes)
        result_context = executor.run(initial_context={})
    """

    def __init__(self, nodes: list[Node], observability: Observability | None = None) -> None:
        self.nodes = nodes
        # Optional, injected by the runtime composition root. Absent -> no telemetry
        # (the documented default for this optional subsystem); the node still runs.
        self._obs = observability

    def run(self, context: dict) -> dict:
        """Execute all nodes in dependency order.

        Parameters
        ----------
        context : dict
            Initial data available to the first nodes (e.g. file paths,
            config). Each node's outputs are merged into this dict after
            the node runs, making them available to downstream nodes.

        Returns
        -------
        dict
            Final context containing all module outputs.

        Raises
        ------
        RuntimeError
            If the graph contains a cycle (nodes that can never be ready).
        """
        context = dict(context)  # shallow copy — don't mutate caller's dict
        completed: set[str] = set()

        max_iterations = len(self.nodes) ** 2 + len(self.nodes) + 1
        iteration = 0

        while len(completed) < len(self.nodes):
            iteration += 1
            if iteration > max_iterations:  # pragma: no cover — preempted by no-progress check
                pending = [n.name for n in self.nodes if n.name not in completed]
                raise RuntimeError(
                    f"Execution graph appears to contain a cycle or unresolvable "
                    f"dependency. Pending nodes: {pending}"
                )

            progress_made = False
            for node in self.nodes:
                if node.name in completed:
                    continue

                # Node is ready when all its dependencies have completed
                ready = all(dep.name in completed for dep in node.dependencies)
                if not ready:
                    continue

                # Validate inputs declared by the module — fail immediately if absent
                for key, validator in (node.module.input_contracts or {}).items():
                    require(
                        key in context,
                        f"Required input '{key}' missing for module '{node.name}'",
                    )
                    validator(context[key])

                # One span per node — the single auto-instrumentation seam. The span's
                # exit captures any propagating exception and records errors_total.
                span_cm = self._obs.span(node.name) if self._obs is not None else nullcontext()
                with span_cm:
                    try:
                        outputs = node.module.run(context)
                    except Exception as exc:
                        # Name the failing stage on the exception so the scan-level
                        # handler reports *which* stage broke. The span still records
                        # the error; we re-raise unchanged (no swallowing, no wrapping).
                        exc.add_note(f"adapt: failing stage = {node.name}")
                        raise

                # Validate outputs declared by the module
                if outputs:
                    for key, validator in (node.module.output_contracts or {}).items():
                        if key in outputs:
                            validator(outputs[key])
                    context.update(outputs)

                completed.add(node.name)
                logger.debug("Completed node: %s", node.name)
                progress_made = True

            if not progress_made and len(completed) < len(self.nodes):
                pending = [n.name for n in self.nodes if n.name not in completed]
                raise RuntimeError(
                    f"No progress made — possible cycle or missing dependency. Pending: {pending}"
                )

        return context
