# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Base interface for all Adapt execution nodes.

Every node in the system — whether in execution/nodes/ or extensions/ — must
declare its name, inputs, outputs, and optionally input/output contracts.
The graph engine uses these declarations to build the execution DAG automatically.

Contract functions come from adapt.contracts — import them there, not here.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar

# Pipeline phase reserved for out-of-band repository enrichment run by the
# PostProcessor (never by the live RadarProcessor, which only knows phases 0–3).
# A module with this phase is selected exclusively by the PostProcessor.
POSTPROCESS_PHASE = 4


class BaseModule(ABC):
    """Abstract base for all Adapt execution nodes.

    Subclasses declare:
    - ``name``: unique identifier used in the execution graph
    - ``inputs``: list of context keys this node reads
    - ``outputs``: list of context keys this node writes
    - ``input_contracts``: optional {key: check_fn} validated before run()
    - ``output_contracts``: optional {key: check_fn} validated after run()

    The graph engine matches ``outputs`` of upstream nodes to ``inputs``
    of downstream nodes to resolve execution order automatically.
    Contract callables (from adapt.contracts) are invoked by GraphExecutor
    automatically — nodes do not call them manually.

    Example::

        from adapt.contracts import check_grid_ds_2d, check_segmented_ds

        class DetectModule(BaseModule):
            name = "detection"
            inputs = ["grid_ds_2d"]
            outputs = ["segmented_ds"]
            input_contracts  = {"grid_ds_2d": check_grid_ds_2d}
            output_contracts = {"segmented_ds": check_segmented_ds}

            def run(self, context):
                grid = context["grid_ds_2d"]
                cells = self._segmenter.segment(grid)
                return {"segmented_ds": cells}
    """

    name: ClassVar[str] = ""
    summary: ClassVar[str] = ""  # one-line role, shown as a config.yaml module comment
    inputs: ClassVar[list[str]] = []
    outputs: ClassVar[list[str]] = []
    input_contracts: ClassVar[dict[str, Callable[[Any], None]]] = {}
    output_contracts: ClassVar[dict[str, Callable[[Any], None]]] = {}
    required_history: ClassVar[int] = 1
    pipeline_phase: ClassVar[int] = 0
    config_class: ClassVar[type | None] = None
    output_table: ClassVar[Any | None] = None
    # Multiple extension tables: maps an output context key -> OutputTableSpec.
    # Used by modules that emit more than one table (e.g. lma). When set it
    # supersedes ``output_table``; the single-table form remains for the common case.
    output_tables: ClassVar[dict[str, Any]] = {}
    injected_global_fields: ClassVar[frozenset[str]] = frozenset()

    @classmethod
    def module_summary(cls) -> str:
        """One-line role for the config.yaml ``modules:`` list comment.

        Prefers the explicit ``summary`` ClassVar, then the first non-empty line of
        the class docstring, falling back to the module ``name``.
        """
        if cls.summary:
            return cls.summary
        for line in (cls.__doc__ or "").strip().splitlines():
            if line.strip():
                return line.strip()
        return cls.name

    @classmethod
    def default_params(cls) -> dict[str, Any]:
        """Owned, user-tunable defaults for this module.

        Sourced from ``config_class`` field defaults — the single place defaults
        live — excluding any field listed in ``injected_global_fields`` (those are
        filled from the shared global section by ``build_config``). Returns ``{}``
        when the module declares no ``config_class``. Feeds dynamic config.yaml
        generation so a new module's params appear without editing any central file.
        """
        if cls.config_class is None:
            return {}
        return {
            name: field.default
            for name, field in cls.config_class.model_fields.items()  # type: ignore[attr-defined]
            if name not in cls.injected_global_fields
        }

    @classmethod
    def param_descriptions(cls) -> dict[str, str]:
        """Map owned param name -> ``Field`` description (for config.yaml comments)."""
        if cls.config_class is None:
            return {}
        return {
            name: (field.description or "")
            for name, field in cls.config_class.model_fields.items()  # type: ignore[attr-defined]
            if name not in cls.injected_global_fields
        }

    @classmethod
    def build_config(cls, internal_config: Any) -> Any | None:
        """Build this module's frozen config by slicing the resolved InternalConfig.

        The module decides what it needs: global values, its own section, and
        (rarely) a cross-reference to another section. Returns None when the
        module needs no config. Core modules override this with the slicing
        logic; the config engine calls it once per registered module at startup.
        """
        return None

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
