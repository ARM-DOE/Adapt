# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Dashboard configuration I/O — pure functions, no Tk, no matplotlib."""

import json
from pathlib import Path

POLL_MS = 10_000  # auto-refresh / live-wait cadence shared by shell and tabs

_USER_CONFIG_PATH = Path.home() / ".adapt" / "user_dashboard.json"
_DEFAULT_CONFIG_PATH = Path(__file__).parent / "dashboard_default_config.json"


def _load_default_config() -> dict:
    """Load bundled dashboard defaults from JSON. Raises if file is missing."""
    with _DEFAULT_CONFIG_PATH.open() as f:
        return json.load(f)


def _save_user_config(
    name: str,
    cfg: dict,
    path: Path = _USER_CONFIG_PATH,
) -> None:
    """Save a named config snapshot to user_dashboard.json."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data: dict = {"configs": {}}
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if "configs" not in data:
            data["configs"] = {}
    data["configs"][name] = cfg
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _load_user_config(name: str, path: Path = _USER_CONFIG_PATH) -> dict:
    """Load a named config from user_dashboard.json. Raises KeyError if absent."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["configs"][name]


def _list_user_configs(path: Path = _USER_CONFIG_PATH) -> list[str]:
    """Return all saved config names. Returns [] if the file does not exist."""
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return list(data.get("configs", {}).keys())


def _load_recent_repos(path: Path = _USER_CONFIG_PATH) -> list[str]:
    """Return the list of recently used repo paths (up to 5)."""
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return list(data.get("recent_repos", []))


def _save_recent_repos(repos: list[str], path: Path = _USER_CONFIG_PATH) -> None:
    """Persist the recent-repos list into user_dashboard.json."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data: dict = {}
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    data["recent_repos"] = repos[:5]
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
