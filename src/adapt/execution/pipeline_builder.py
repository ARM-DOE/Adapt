"""NEXRAD pipeline assembly via ModuleRegistry + GraphBuilder.

This module shows how the controller builds the full processing pipeline
from registered modules. It is the bridge between the module system and
the graph execution engine.

The pipeline is assembled once at startup; the graph executor runs it
once per radar file.

Usage::

    pipeline = NexradPipeline(config)
    result = pipeline.process_file(nexrad_file_path, repository=repo)
    cells_df = result["cell_stats"]
"""

import importlib
import logging
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import yaml

from adapt.execution.module_registry import registry
from adapt.execution.graph.builder import GraphBuilder
from adapt.execution.graph.executor import GraphExecutor

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig
    from adapt.persistence.repository import DataRepository

logger = logging.getLogger(__name__)

_DEFAULTS_YAML = Path(__file__).parent.parent / "configuration" / "defaults.yaml"


def _ensure_modules_registered() -> None:
    """Import module files listed in config/defaults.yaml.

    Each entry under ``pipeline.modules`` is a Python module path.
    Importing it triggers the ``registry.register()`` call at module level.
    To add a new module: add one line to defaults.yaml — no Python edits needed.
    """
    try:
        with open(_DEFAULTS_YAML) as f:
            cfg = yaml.safe_load(f)
        module_paths = cfg.get("pipeline", {}).get("modules", [])
    except Exception as e:
        logger.warning("Could not read defaults.yaml (%s); falling back to hardcoded list", e)
        module_paths = [
            "adapt.modules.ingest.module",
            "adapt.modules.detection.module",
            "adapt.modules.projection.module",
            "adapt.modules.analysis.module",
            "adapt.modules.tracking.module",
        ]

    for path in module_paths:
        try:
            importlib.import_module(path)
            logger.debug("Registered module from: %s", path)
        except Exception as e:
            logger.error("Failed to import module '%s': %s", path, e)


class NexradPipeline:
    """Graph-based NEXRAD processing pipeline.

    Assembles the execution graph from the module registry and runs it
    once per radar file. Module instances persist across files so that
    stateful modules (e.g. ProjectionModule with frame history) work
    correctly.

    Parameters
    ----------
    config : InternalConfig
        Runtime configuration forwarded to modules via the context dict.
    output_dirs : dict, optional
        Output directory mapping forwarded to modules via context.

    Example::

        pipeline = NexradPipeline(config, output_dirs=dirs)
        result = pipeline.process_file("KLOT20240518_123456_V06", repo)
        print(result["cell_stats"].head())
    """

    def __init__(
        self,
        config: "InternalConfig",
        output_dirs: Optional[dict] = None,
    ) -> None:
        self.config = config
        self.output_dirs = output_dirs or {}

        _ensure_modules_registered()

        # Build a local registry with only NEXRAD pipeline modules
        # (avoids polluting the global registry if it already has modules)
        local_modules = registry.create_modules()
        self._nodes = GraphBuilder(local_modules).build()
        self._executor = GraphExecutor(self._nodes)
        logger.info(
            "NexradPipeline assembled: %s",
            " → ".join(n.name for n in self._nodes),
        )

    def process_file(
        self,
        nexrad_file: str,
        repository: Optional["DataRepository"] = None,
    ) -> dict:
        """Run the full processing graph for a single NEXRAD file.

        Parameters
        ----------
        nexrad_file : str
            Path to the NEXRAD Level-II file.
        repository : DataRepository, optional
            If provided, analysis results are persisted automatically.

        Returns
        -------
        dict
            Final context dict with keys: grid_ds, grid_ds_2d, segmented_ds,
            projected_ds, cell_stats, scan_time.
        """
        context = {
            "nexrad_file": nexrad_file,
            "config": self.config,
            "output_dirs": self.output_dirs,
        }
        if repository is not None:
            context["repository"] = repository

        return self._executor.run(context)
