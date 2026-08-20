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

    def scan_index(self) -> list[tuple[str, str, Path]]:
        """Ordered ``(scan_id, scan_time, path)`` for the radar's analysis scans.

        Discovery goes through the catalog — never by walking directories or
        parsing filenames. Restricted to the selected run; all runs when no run
        is selected. An empty list means the radar has no cataloged data yet.
        """
        repo = Path(self.repo())
        radar = self.radar()
        if not (repo / radar / "catalog.db").exists():
            return []  # no data yet for this radar
        df = self.client().artifacts(
            product_type="segmentation2d", radar=radar, run_id=self.run_id()
        )
        if df.empty:
            return []
        return [
            (str(r.scan_id), str(r.scan_time), repo / radar / str(r.file_path))
            for r in df.itertuples()
        ]

    def nc_files(self) -> list[Path]:
        """Analysis NC paths in scan order — a path view of ``scan_index``."""
        return [path for _, _, path in self.scan_index()]

    def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None
