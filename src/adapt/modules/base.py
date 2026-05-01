# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Base interface for all Adapt processing modules.

Every module in the system — whether in modules/ or extensions/ — must
declare its name, inputs, and outputs. The graph engine uses these
declarations to build the execution DAG automatically.

Existing scientific classes (AwsNexradDownloader, RadarCellSegmenter, etc.)
are NOT required to inherit BaseModule in Step 1. They are wrapped in
Step 6 of the refactor. BaseModule is the target interface definition.
"""

from abc import ABC, abstractmethod
from typing import ClassVar

from adapt.contracts import ContractViolation, require  # noqa: F401 — re-exported for callers

# ────────────────────────────────────────────────────────────────────────────
# BaseModule Interface
# ────────────────────────────────────────────────────────────────────────────


class BaseModule(ABC):
    """Abstract base for all Adapt processing modules.

    Subclasses declare:
    - ``name``: unique identifier used in the execution graph
    - ``inputs``: list of data keys this module reads from context
    - ``outputs``: list of data keys this module writes to context
    - ``input_contracts``: optional {key: callable} validators run before run()
    - ``output_contracts``: optional {key: callable} validators run after run()

    The graph engine matches ``outputs`` of upstream modules to ``inputs``
    of downstream modules to resolve execution order automatically.
    Contract callables are invoked by GraphExecutor automatically — modules
    do not need to call them manually.

    Example::

        class DetectModule(BaseModule):
            name = "detection"
            inputs = ["grid_ds_2d"]
            outputs = ["segmented_ds"]
            input_contracts  = {"grid_ds_2d": assert_gridded}
            output_contracts = {"segmented_ds": assert_segmented}

            def run(self, context):
                grid = context["grid_ds_2d"]
                cells = self._segmenter.segment(grid)
                return {"segmented_ds": cells}
    """

    name: ClassVar[str] = ""
    inputs: ClassVar[list[str]] = []
    outputs: ClassVar[list[str]] = []
    input_contracts:  ClassVar[dict[str, object]] = {}
    output_contracts: ClassVar[dict[str, object]] = {}

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
