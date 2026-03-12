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

import logging
from typing import Optional, TYPE_CHECKING

from adapt.controller.module_registry import registry
from adapt.graph.graph_builder import GraphBuilder
from adapt.graph.graph_executor import GraphExecutor

if TYPE_CHECKING:
    from adapt.schemas import InternalConfig
    from adapt.core.data_repository import DataRepository

logger = logging.getLogger(__name__)


def _ensure_modules_registered() -> None:
    """Import module files so their registry.register() calls run."""
    import adapt.modules.load.module      # noqa: F401 — registers LoadModule
    import adapt.modules.detect.module    # noqa: F401 — registers DetectModule
    import adapt.modules.projection.module  # noqa: F401 — registers ProjectionModule
    import adapt.modules.analysis.module  # noqa: F401 — registers AnalysisModule


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
