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
from typing import ClassVar, Dict, List, Optional


# ────────────────────────────────────────────────────────────────────────────
# Contract Enforcement Infrastructure
# ────────────────────────────────────────────────────────────────────────────


class ContractViolation(RuntimeError):
    """Raised when a pipeline contract is violated.

    This indicates a bug in pipeline logic, not bad user input or recoverable
    science edge cases. It means a pipeline stage did not produce the invariants
    it promised.

    Key distinction:
    - ValueError: User/config error (handled by Pydantic)
    - ContractViolation: Pipeline bug (programmer error)
    - Exception: Recoverable science issues (try/except in algorithms)
    """
    pass


def require(condition: bool, message: str) -> None:
    """Enforce a pipeline contract.

    This is called at stage boundaries to verify the preceding stage
    produced the guaranteed invariants. It is fail-fast: no recovery,
    no fallback, no silence.

    Parameters
    ----------
    condition : bool
        The invariant that must be true. If False, ContractViolation is raised.

    message : str
        Error message explaining the contract violation (for debugging).

    Raises
    ------
    ContractViolation
        If condition is False. This indicates a bug in pipeline logic.

    Examples
    --------
    >>> require("x" in ds.coords, "Grid contract: missing 'x' coordinate")
    >>> require(df.shape[0] > 0, "Analysis contract: at least one cell expected")
    """
    if not condition:
        raise ContractViolation(message)


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
    inputs: ClassVar[List[str]] = []
    outputs: ClassVar[List[str]] = []
    input_contracts:  ClassVar[Dict[str, object]] = {}
    output_contracts: ClassVar[Dict[str, object]] = {}

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
