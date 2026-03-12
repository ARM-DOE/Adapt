"""Builds an execution DAG from a list of modules.

The builder inspects each module's declared ``inputs`` and ``outputs``,
then wires nodes together so that every node's dependencies point to the
nodes that produce its required inputs.
"""

from typing import Dict, List, TYPE_CHECKING

from adapt.graph.node import Node

if TYPE_CHECKING:
    from adapt.modules.base import BaseModule


class GraphBuilder:
    """Construct a directed acyclic graph from module declarations.

    Parameters
    ----------
    modules : list[BaseModule]
        Ordered or unordered list of modules to connect. Execution order
        is determined by input/output matching, not list order.

    Raises
    ------
    ValueError
        If two modules declare the same output key (ambiguous dependency).

    Example::

        builder = GraphBuilder([download_mod, load_mod, grid_mod, detect_mod])
        nodes = builder.build()
    """

    def __init__(self, modules: List["BaseModule"]) -> None:
        self.modules = modules

    def build(self) -> List[Node]:
        """Build and return the list of connected nodes.

        Returns
        -------
        list[Node]
            All nodes with ``dependencies`` and ``dependents`` populated.
            Nodes are returned in insertion order; execution order is
            determined by the GraphExecutor.
        """
        nodes: Dict[str, Node] = {m.name: Node(m) for m in self.modules}

        # Map each output key → the node that produces it
        output_map: Dict[str, Node] = {}
        for node in nodes.values():
            for output in node.outputs:
                if output in output_map:
                    raise ValueError(
                        f"Output '{output}' declared by both "
                        f"'{output_map[output].name}' and '{node.name}'. "
                        "Each output key must be unique."
                    )
                output_map[output] = node

        # Wire dependencies
        for node in nodes.values():
            for inp in node.inputs:
                if inp in output_map:
                    parent = output_map[inp]
                    if parent not in node.dependencies:
                        node.dependencies.append(parent)
                    if node not in parent.dependents:
                        parent.dependents.append(node)

        return list(nodes.values())
