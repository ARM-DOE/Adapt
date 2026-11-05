"""Tile-server identity and cache location shared by every basemap consumer.

Tile providers require a stable User-Agent that names the application and a
contact (OSM blocks anonymous or rotating agents with 403), and they require
tiles to be cached rather than re-downloaded on every run.
"""

from importlib.metadata import version
from pathlib import Path

from adapt.utils.basemap import TILE_CACHE_DIR, tile_headers


def test_user_agent_names_adapt_with_version_and_contact():
    ua = tile_headers()["user-agent"]
    assert ua.startswith(f"Adapt/{version('arm-adapt')} ")
    assert "https://" in ua  # contact URL, as the OSM tile usage policy requires


def test_user_agent_overrides_contextily_default_when_merged():
    # contextily builds ``{"user-agent": <random uuid>, **headers}``: the same
    # lower-case key must win, or the anonymous agent would still be sent.
    merged = {"user-agent": "contextily-deadbeef", **tile_headers()}
    assert merged["user-agent"].startswith("Adapt/")


def test_tile_cache_lives_in_central_adapt_directory():
    assert Path.home() / ".adapt" / "tiles" == TILE_CACHE_DIR
