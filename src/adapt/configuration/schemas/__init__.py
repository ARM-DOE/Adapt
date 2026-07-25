# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Complete runtime initialization for Adapt pipeline.

This module handles configuration resolution, directory setup, cleanup,
and persistence. It provides a single entry point for complete runtime
initialization.

Exports
-------
init_runtime_config : function
    Complete runtime initialization - the ONLY public function
"""

from adapt.configuration.schemas.errors import ConfigError
from adapt.configuration.schemas.initialization import init_runtime_config
from adapt.configuration.schemas.module_resolver import resolve_module_configs

__all__ = ["ConfigError", "init_runtime_config", "resolve_module_configs"]
