"""Complete runtime initialization for Adapt pipeline.

This module handles configuration resolution, directory setup, cleanup,
and persistence. It provides a single entry point for complete runtime
initialization.

Exports
-------
init_runtime_config : function
    Complete runtime initialization - the ONLY public function
"""

from adapt.schemas.initialization import init_runtime_config

# Single public function - everything else is internal implementation
__all__ = ['init_runtime_config']
