# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Out-of-band repository enrichment by composing the live pipeline's infra.

``PostProcessor`` enriches an already-processed repository with *extension*
tables (e.g. lightning stats), without touching the proven ``RadarProcessor``.
Its only conceptual difference from the live processor is that its inputs already
live in the repository rather than coming from raw radar.

It reuses — by composition, not inheritance — the same components the pipeline
uses: the module ``registry``, ``GraphBuilder``/``GraphExecutor`` for dependency
resolution, ``resolve_module_configs`` for config, and the ``OutputRouter`` for
persistence (whose SqliteTable writer enforces that only extension tables, never
core tables, are written). Post-process modules declare
``pipeline_phase = POSTPROCESS_PHASE`` and are selected exclusively here; the
live pipeline never loads them.
"""

import importlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from adapt.configuration.schemas.module_resolver import resolve_module_configs
from adapt.contracts import PersistenceMeta
from adapt.execution.graph.builder import GraphBuilder
from adapt.execution.graph.executor import GraphExecutor
from adapt.execution.module_registry import registry
from adapt.execution.pipeline_builder import resolve_enabled_modules
from adapt.modules.base import POSTPROCESS_PHASE
from adapt.persistence.output_router import OutputRouter

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
        with open(_POSTPROCESS_DEFAULTS_YAML, encoding="utf-8") as f:
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
        ``grid_ds_3d``): ``minute_masks`` (minute-resolution registration masks —
        the geometry product every external-association module consumes) and
        ``radar_origin`` (the radar lat/lon for projection).
        """
        context = {
            "run_id": self.repository.run_id,
            "repository": self.repository,
            "catalog_path": self.repository.catalog.db_path,
        }
        context.update(resolve_module_configs(self.config))

        declared = {key for module in selected for key in module.inputs}
        if "minute_masks" in declared:
            from adapt.persistence.scan_mask_reader import read_minute_masks

            context["minute_masks"] = read_minute_masks(self.repository)
        if "radar_origin" in declared:
            lat, lon = self.repository.registry.get_radar_location(self.repository.radar)
            context["radar_origin"] = (lat, lon)
        return context

    def _persist(self, modules: list, result: dict) -> None:
        """Route each module's declared persistence specs through the OutputRouter.

        Run-level persist: there is no single scan_time (rows carry their own),
        so ``meta.scan_time`` is None and any time-stamped artifact spec would
        correctly raise.
        """
        meta = PersistenceMeta(
            scan_time=None,
            run_id=self.repository.run_id,
            source_file="",
            dataset_id=self.repository.radar,
        )
        OutputRouter(self.repository).persist(modules, result, meta)
