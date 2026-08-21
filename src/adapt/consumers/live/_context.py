# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""AppContext — the only thing the dashboard's tabs know about the app shell.

Carries the session facts every tab needs (store/collection/run selection,
dashboard config, one cached read-only StoreClient, the scan timeline) as
plain callables and methods, so tabs never reach into the shell or each other.
Tabs never see paths or artifact ids — rasters arrive in-memory.
"""

import contextlib
from collections.abc import Callable
from datetime import datetime

from adapt.api.domain import ScanRaster, ScanRef
from adapt.api.store_client import StoreClient


class AppContext:
    """Session facts shared with tabs: selection state, config, store access."""

    def __init__(
        self,
        *,
        get_repo: Callable[[], str],
        get_collection: Callable[[], str],
        get_run_sel: Callable[[], str],
        get_cfg: Callable[[], dict],
        report_scan_time: Callable[[datetime], None],
    ):
        self._get_repo = get_repo
        self._get_collection = get_collection
        self._get_run_sel = get_run_sel
        self._get_cfg = get_cfg
        self.report_scan_time = report_scan_time
        self._client: StoreClient | None = None
        self._client_repo: str | None = None
        self._tracking_fields: dict[str, str] = {}

    def repo(self) -> str:
        return self._get_repo().strip()

    def collection(self) -> str:
        return self._get_collection().strip().upper()

    def run_id(self) -> str | None:
        """Run id parsed from the toolbar Run selector, or None if unset."""
        sel = self._get_run_sel().strip()
        return sel.split()[0] if sel else None

    def cfg(self) -> dict:
        """Live view of the shell-owned dashboard config (Load Config swaps it)."""
        return self._get_cfg()

    def client(self) -> StoreClient:
        """One StoreClient per store root, replaced (and closed) on change."""
        repo = self.repo()
        if self._client is None or self._client_repo != repo:
            if self._client is not None:
                with contextlib.suppress(Exception):
                    self._client.close()
            self._client = StoreClient(repo)
            self._client_repo = repo
            self._tracking_fields.clear()  # run ids are only unique per store
        return self._client

    def tracking_field(self, run_id: str | None = None) -> str:
        """The canonical field the given (or active) run tracked on.

        Resolved once per run from stored config provenance — the store is
        the truth for what a run tracked; never silently assumed.
        """
        rid = run_id or self.active_run_id()
        if not rid:
            raise ValueError("tracking_field: no run selected")
        if rid not in self._tracking_fields:
            self._tracking_fields[rid] = self.client().run_tracking_field(rid)
        return self._tracking_fields[rid]

    def active_run_id(self) -> str | None:
        """The selected run, else the collection's latest run, else None."""
        selected = self.run_id()
        if selected:
            return selected
        latest = self.client().latest_run(self.collection())
        return latest.run_id if latest else None

    def timeline(self) -> list[ScanRef]:
        """The active run's ordered, complete-only scan timeline.

        Discovery goes through the catalog — never by walking directories or
        parsing filenames. An empty list means no complete scans yet.
        """
        run_id = self.active_run_id()
        if run_id is None:
            return []
        return self.client().scan_timeline(self.collection(), run_id)

    def open_raster(self, ref: ScanRef, product: str = "segmentation2d") -> ScanRaster:
        """One scan's raster, fully in memory — close it (or use ``with``) per frame."""
        return self.client().open_scan_raster(self.collection(), ref.run_id, ref.scan_id, product)

    def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None
