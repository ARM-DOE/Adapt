# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for dashboard JSON config loading and persistence."""

import json

import pytest

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# _load_default_config
# ---------------------------------------------------------------------------


def test_load_default_config_returns_dict_with_required_keys():
    """_load_default_config must return a dict containing all required top-level keys."""
    from adapt.consumers.live._config import _load_default_config

    cfg = _load_default_config()

    assert isinstance(cfg, dict)
    for key in ("colors", "plot_groups", "plot_assignments", "poll_ms", "overflow_action"):
        assert key in cfg, f"required key '{key}' missing from default config"


def test_load_default_config_colors_are_seven_hex_strings():
    """colors list must contain exactly 7 valid hex color strings."""
    from adapt.consumers.live._config import _load_default_config

    cfg = _load_default_config()
    colors = cfg["colors"]

    assert len(colors) == 7
    for c in colors:
        assert c.startswith("#") and len(c) == 7, f"'{c}' is not a valid #RRGGBB hex color"


def test_load_default_config_plot_groups_contains_required_groups():
    """plot_groups must define Area, Reflectivity, ZDR, and Velocity groups."""
    from adapt.consumers.live._config import _load_default_config

    cfg = _load_default_config()
    groups = cfg["plot_groups"]

    for name in ("Area", "Reflectivity", "ZDR", "Velocity"):
        assert name in groups, f"required plot group '{name}' missing"
        assert "variables" in groups[name]
        assert "styles" in groups[name]
        assert "labels" in groups[name]


def test_load_default_config_has_lightning_group():
    """A selectable Lightning group must source from lma_cell_stats columns."""
    from adapt.consumers.live._config import _load_default_config

    cfg = _load_default_config()
    groups = cfg["plot_groups"]

    assert "Lightning" in groups
    assert set(groups["Lightning"]["variables"]) == {"flash_count", "source_count"}
    assert len(groups["Lightning"]["styles"]) == len(groups["Lightning"]["variables"])
    assert len(groups["Lightning"]["labels"]) == len(groups["Lightning"]["variables"])


def test_load_default_config_plot_assignments_has_three_entries():
    """plot_assignments must list exactly 3 group names (one per timeline plot slot)."""
    from adapt.consumers.live._config import _load_default_config

    cfg = _load_default_config()
    assignments = cfg["plot_assignments"]

    assert len(assignments) == 3
    for name in assignments:
        assert name in cfg["plot_groups"], f"assignment '{name}' not in plot_groups"


# ---------------------------------------------------------------------------
# _save_user_config / _load_user_config
# ---------------------------------------------------------------------------


@pytest.fixture()
def user_config_path(tmp_path):
    """Yield a temp path for user_dashboard.json, clean up after."""
    yield tmp_path / "user_dashboard.json"


def test_save_user_config_creates_file_with_named_entry(user_config_path):
    """_save_user_config must write a named config entry to user_dashboard.json."""
    from adapt.consumers.live._config import _load_default_config, _save_user_config

    cfg = _load_default_config()
    _save_user_config("my config", cfg, user_config_path)

    assert user_config_path.exists()
    data = json.loads(user_config_path.read_text())
    assert "configs" in data
    assert "my config" in data["configs"]


def test_save_user_config_second_save_adds_entry_without_losing_first(user_config_path):
    """Saving a second named config must not overwrite the first."""
    from adapt.consumers.live._config import _load_default_config, _save_user_config

    cfg = _load_default_config()
    _save_user_config("first", cfg, user_config_path)
    _save_user_config("second", cfg, user_config_path)

    data = json.loads(user_config_path.read_text())
    assert "first" in data["configs"]
    assert "second" in data["configs"]


def test_load_user_config_returns_saved_config(user_config_path):
    """_load_user_config must return the exact config that was saved."""
    from adapt.consumers.live._config import (
        _load_default_config,
        _load_user_config,
        _save_user_config,
    )

    cfg = _load_default_config()
    cfg["poll_ms"] = 99999  # distinct marker value
    _save_user_config("event", cfg, user_config_path)

    loaded = _load_user_config("event", user_config_path)

    assert loaded["poll_ms"] == 99999


def test_load_user_config_raises_when_name_missing(user_config_path):
    """_load_user_config must raise KeyError for a config name that was never saved."""
    from adapt.consumers.live._config import _load_user_config

    user_config_path.write_text(json.dumps({"configs": {}}))

    with pytest.raises(KeyError):
        _load_user_config("nonexistent", user_config_path)


def test_list_user_configs_returns_saved_names(user_config_path):
    """_list_user_configs must return the names of all saved configs."""
    from adapt.consumers.live._config import (
        _list_user_configs,
        _load_default_config,
        _save_user_config,
    )

    cfg = _load_default_config()
    _save_user_config("alpha", cfg, user_config_path)
    _save_user_config("beta", cfg, user_config_path)

    names = _list_user_configs(user_config_path)

    assert set(names) == {"alpha", "beta"}


def test_list_user_configs_returns_empty_list_when_file_absent(tmp_path):
    """_list_user_configs must return [] when user_dashboard.json does not exist."""
    from adapt.consumers.live._config import _list_user_configs

    missing = tmp_path / "no_such_file.json"
    assert _list_user_configs(missing) == []
