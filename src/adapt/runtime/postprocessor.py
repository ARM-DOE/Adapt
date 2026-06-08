# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Out-of-band repository enrichment by composing the live pipeline's infra.

``PostProcessor`` enriches an already-processed repository with *extension*
tables (e.g. lightning stats), without touching the proven ``RadarProcessor``.
Its only conceptual difference from the live processor is that its inputs already
live in the repository rather than coming from raw radar.

It reuses — by composition, not inheritance — the same components the pipeline
uses: the module ``registry``, ``GraphBuilder``/``GraphExecutor`` for dependency
resolution, ``resolve_module_configs`` for config, and ``ModuleOutputWriter`` for
persistence (which enforces that only extension tables, never core tables, are
written). Post-process modules declare ``pipeline_phase = POSTPROCESS_PHASE`` and
are selected exclusively here; the live pipeline never loads them.
"""

import importlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from adapt.configuration.schemas.module_resolver import resolve_module_configs
from adapt.execution.graph.builder import GraphBuilder
from adapt.execution.graph.executor import GraphExecutor
from adapt.execution.module_registry import registry
from adapt.execution.pipeline_builder import resolve_enabled_modules
from adapt.modules.base import POSTPROCESS_PHASE
from adapt.persistence.module_output import ModuleOutputWriter

if TYPE_CHECKING:
    from adapt.configuration.schemas.internal import InternalConfig
    from adapt.persistence.repository import DataRepository

logger = logging.getLogger(__name__)

_POSTPROCESS_DEFAULTS_YAML = (
    Path(__file__).parent.parent / "configuration" / "postprocess_defaults.yaml"
)


def _ensure_postprocess_modules_registered(extensions: list[str] | None = None) -> None:
    """Import post-process module paths from postprocess_defaults.yaml + extensions.

    Importing each path triggers its ``registry.register()`` at module level.
    Mirrors the pipeline's ``_ensure_modules_registered``; kept separate so the
    live pipeline never imports (and thus never sees) post-process modules.
    """
    module_paths: list[str] = []
    if _POSTPROCESS_DEFAULTS_YAML.exists():
        with open(_POSTPROCESS_DEFAULTS_YAML) as f:
            cfg = yaml.safe_load(f) or {}
        module_paths = cfg.get("postprocess", {}).get("modules", []) or []

    for path in module_paths:
        importlib.import_module(path)
        logger.debug("Registered post-process module from: %s", path)

    for path in extensions or []:
        try:
            importlib.import_module(path)
            logger.info("Registered post-process extension from: %s", path)
        except Exception as e:
            raise ImportError(f"Failed to load post-process extension '{path}': {e}") from e


class PostProcessor:
    """Run post-process (phase ``POSTPROCESS_PHASE``) modules over a repository."""

    def __init__(
        self,
        repository: "DataRepository",
        config: "InternalConfig",
        extensions: list[str] | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self._extensions = extensions

    def run(self, modules: list[str]) -> dict:
        """Resolve, execute, and persist the requested post-process modules.

        Parameters
        ----------
        modules : list[str]
            Names of post-process modules to run (from ``--module`` or the
            ``postprocess.modules`` config section).
        """
        _ensure_postprocess_modules_registered(self._extensions)

        available = [m for m in registry.create_modules() if m.pipeline_phase == POSTPROCESS_PHASE]
        selected = resolve_enabled_modules(available, modules=modules)
        logger.info("Post-processing modules: [%s]", ", ".join(m.name for m in selected))

        nodes = GraphBuilder(selected).build()
        context = self._build_context(selected)
        result = GraphExecutor(nodes).run(context)

        self._persist(selected, result)
        return result

    def _build_context(self, selected: list) -> dict:
        """Repository-level base context, module configs, and on-demand repo inputs.

        Repository-backed inputs are injected only when a selected module declares
        them (the same on-demand pattern the live processor uses for
        ``grid_ds_3d``): ``scan_masks`` (per-scan cell masks read from the
        analysis NetCDFs) and ``radar_origin`` (the radar lat/lon for projection).
        """
        context = {
            "run_id": self.repository.run_id,
            "repository": self.repository,
            "catalog_path": self.repository.catalog.db_path,
        }
        context.update(resolve_module_configs(self.config))

        declared = {key for module in selected for key in module.inputs}
        if "scan_masks" in declared:
            from adapt.persistence.scan_mask_reader import read_scan_masks

            context["scan_masks"] = read_scan_masks(self.repository)
        if "radar_origin" in declared:
            lat, lon = self.repository.registry.get_radar_location(self.repository.radar)
            context["radar_origin"] = (lat, lon)
        return context

    def _persist(self, modules: list, result: dict) -> None:
        """Write each module's declared extension table(s) from its returned frames."""
        db_path = self.repository.catalog.db_path
        for module in modules:
            for output_key, spec in self._output_specs(module).items():
                df = result.get(output_key)
                if df is None or getattr(df, "empty", True):
                    continue
                ModuleOutputWriter(db_path, spec).write(df)

    @staticmethod
    def _output_specs(module) -> dict:
        """Map each output context key to its OutputTableSpec.

        Supports modules with multiple tables (``output_tables``) and the common
        single-table form (``output_table`` bound to the first output).
        """
        if module.output_tables:
            return dict(module.output_tables)
        if module.output_table is not None and module.outputs:
            return {module.outputs[0]: module.output_table}
        return {}
