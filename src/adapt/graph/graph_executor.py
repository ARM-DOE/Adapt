"""Executes a DAG of nodes in dependency order.

The executor runs a simple topological loop: nodes whose dependencies have
all completed are eligible to run. This single-threaded executor is the
reference implementation. Future steps may add parallel execution.
"""

from typing import List, Set

from adapt.graph.node import Node


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

    def __init__(self, nodes: List[Node]) -> None:
        self.nodes = nodes

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
        completed: Set[str] = set()

        max_iterations = len(self.nodes) ** 2 + len(self.nodes) + 1
        iteration = 0

        while len(completed) < len(self.nodes):
            iteration += 1
            if iteration > max_iterations:
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

                outputs = node.module.run(context)
                if outputs:
                    context.update(outputs)
                completed.add(node.name)
                progress_made = True

            if not progress_made and len(completed) < len(self.nodes):
                pending = [n.name for n in self.nodes if n.name not in completed]
                raise RuntimeError(
                    f"No progress made — possible cycle or missing dependency. "
                    f"Pending: {pending}"
                )

        return context
