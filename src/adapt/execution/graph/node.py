# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Execution graph node.

Each node wraps a module and tracks its position in the DAG: which nodes
it depends on (must complete before this node runs) and which nodes depend
on it (notified when this node completes).
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from adapt.modules.base import BaseModule


class Node:
    """A node in the execution graph.

    Parameters
    ----------
    module : BaseModule
        The processing module this node wraps.

    Attributes
    ----------
    module : BaseModule
    inputs : list[str]
        Data keys required by this module.
    outputs : list[str]
        Data keys produced by this module.
    dependencies : list[Node]
        Upstream nodes that must complete before this one runs.
    dependents : list[Node]
        Downstream nodes that are unblocked when this one completes.
    """

    def __init__(self, module: "BaseModule") -> None:
        self.module = module
        self.inputs: list[str] = list(module.inputs)
        self.outputs: list[str] = list(module.outputs)
        self.dependencies: list[Node] = []
        self.dependents: list[Node] = []

    @property
    def name(self) -> str:
        return self.module.name

    def __repr__(self) -> str:
        return (
            f"Node(name={self.name!r}, "
            f"inputs={self.inputs}, outputs={self.outputs})"
        )
