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

from adapt.configuration.schemas.initialization import init_runtime_config
from adapt.configuration.schemas.materialization import materialize_module_configs

__all__ = ['init_runtime_config', 'materialize_module_configs']
