# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""AppContext — the only thing the dashboard's tabs know about the app shell.

Carries the session facts every tab needs (repo/radar/run selection, dashboard
config, one cached read-only RepositoryClient, the analysis-file timeline) as
plain callables and methods, so tabs never reach into the shell or each other.
"""

import contextlib
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from adapt.api.client import RepositoryClient
from adapt.consumers.live._targeting import filter_nc_paths_by_run


class AppContext:
    """Session facts shared with tabs: selection state, config, repository access."""

    def __init__(
        self,
        *,
        get_repo: Callable[[], str],
        get_radar: Callable[[], str],
        get_run_sel: Callable[[], str],
        get_cfg: Callable[[], dict],
        report_scan_time: Callable[[datetime], None],
    ):
        self._get_repo = get_repo
        self._get_radar = get_radar
        self._get_run_sel = get_run_sel
        self._get_cfg = get_cfg
        self.report_scan_time = report_scan_time
        self._client: RepositoryClient | None = None
        self._client_repo: str | None = None

    def repo(self) -> str:
        return self._get_repo().strip()

    def radar(self) -> str:
        return self._get_radar().strip().upper()

    def run_id(self) -> str | None:
        """Run id parsed from the toolbar Run selector, or None if unset."""
        sel = self._get_run_sel().strip()
        return sel.split()[0] if sel else None

    def cfg(self) -> dict:
        """Live view of the shell-owned dashboard config (Load Config swaps it)."""
        return self._get_cfg()

    def client(self) -> RepositoryClient:
        """One RepositoryClient per repo path, replaced (and closed) on change."""
        repo = self.repo()
        if self._client is None or self._client_repo != repo:
            if self._client is not None:
                with contextlib.suppress(Exception):
                    self._client.close()
            self._client = RepositoryClient(repo)
            self._client_repo = repo
        return self._client

    def nc_files(self) -> list[Path]:
        """All analysis NC files for the radar, chronological, restricted to the
        selected run (legacy files without a run id fall back to the full list)."""
        analysis_dir = Path(self.repo()) / self.radar() / "analysis"
        if not analysis_dir.exists():
            return []
        all_nc: list[Path] = []
        for date_dir in list(analysis_dir.iterdir()):  # eager: release FD immediately
            if date_dir.is_dir() and len(date_dir.name) == 8 and date_dir.name.isdigit():
                all_nc.extend(list(date_dir.glob("*_analysis.nc")))  # eager
        all_nc = sorted(all_nc, key=lambda p: p.name)
        filtered = filter_nc_paths_by_run(all_nc, self.run_id())
        return filtered if filtered else all_nc

    def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None
