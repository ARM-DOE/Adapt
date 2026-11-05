# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tile-server identity and cache location shared by every basemap consumer.

Tile providers require a stable User-Agent naming the application and a
contact (OSM answers anonymous or rotating agents with 403) and require tiles
to be cached rather than re-downloaded on every run. The consumers that call
contextily pass ``tile_headers()`` on every fetch and point contextily's tile
cache at ``TILE_CACHE_DIR`` once at import.
"""

from importlib.metadata import version
from pathlib import Path

TILE_CACHE_DIR = Path.home() / ".adapt" / "tiles"

_CONTACT = "https://github.com/RBhupi/Adapt"


def tile_headers() -> dict[str, str]:
    """HTTP headers for tile requests. The key is lower-case on purpose: contextily
    merges ``{"user-agent": <random uuid>, **headers}``, so only an identical key
    replaces its anonymous agent."""
    return {"user-agent": f"Adapt/{version('arm-adapt')} (+{_CONTACT})"}
