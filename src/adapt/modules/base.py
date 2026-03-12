"""Base interface for all Adapt processing modules.

Every module in the system — whether in modules/ or extensions/ — must
declare its name, inputs, and outputs. The graph engine uses these
declarations to build the execution DAG automatically.

Existing scientific classes (AwsNexradDownloader, RadarCellSegmenter, etc.)
are NOT required to inherit BaseModule in Step 1. They are wrapped in
Step 6 of the refactor. BaseModule is the target interface definition.
"""

from abc import ABC, abstractmethod
from typing import ClassVar, List


class BaseModule(ABC):
    """Abstract base for all Adapt processing modules.

    Subclasses declare:
    - ``name``: unique identifier used in the execution graph
    - ``inputs``: list of data keys this module reads from context
    - ``outputs``: list of data keys this module writes to context

    The graph engine matches ``outputs`` of upstream modules to ``inputs``
    of downstream modules to resolve execution order automatically.

    Example::

        class DetectModule(BaseModule):
            name = "detect"
            inputs = ["grid_volume"]
            outputs = ["storm_cells"]

            def run(self, context):
                grid = context["grid_volume"]
                cells = self._segmenter.segment(grid)
                return {"storm_cells": cells}
    """

    name: ClassVar[str] = ""
    inputs: ClassVar[List[str]] = []
    outputs: ClassVar[List[str]] = []

    @abstractmethod
    def run(self, context: dict) -> dict:
        """Execute this module.

        Parameters
        ----------
        context : dict
            Shared data store. Keys declared in ``inputs`` are guaranteed
            to be present (populated by upstream modules).

        Returns
        -------
        dict
            Keys declared in ``outputs``, populated by this module.
            The graph executor merges these into the shared context.
        """
        ...
