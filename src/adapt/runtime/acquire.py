# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Acquisition gateway: raw scans become cataloged store objects at the boundary.

The runtime builds one ``StoreAcquirer`` per run and injects it into the
ingress source — acquisition modules never import persistence. Acquisition is
idempotent per ``source_uri`` within a collection: bytes already in the store
are reused (never rewritten), and the scan is registered for the acquiring
run. The returned message is the processor queue contract:
``{artifact_id, scan_id, scan_time, queued_at}``.
"""

import time
from datetime import datetime
from pathlib import Path

from adapt.contracts.persistence import ScanRecord
from adapt.persistence.errors import StoreError
from adapt.persistence.objects import ArtifactMeta, ObjectStore
from adapt.persistence.store import Collection
from adapt.utils.identity import scan_id_from_bytes

__all__ = ["StoreAcquirer"]


class StoreAcquirer:
    """Commit raw scans into one collection's store, once, at the source boundary."""

    def __init__(self, collection: Collection, run_id: str) -> None:
        self._collection = collection
        self._objects = ObjectStore(collection.objects_dir, collection.catalog)
        self._run_id = run_id

    def is_acquired(self, source_uri: str) -> bool:
        """True when this source's bytes are already objects in the store."""
        return self._collection.catalog.find_by_source_uri(source_uri) is not None

    def acquire_file(
        self, path: str | Path, *, source_uri: str, scan_time: datetime | None
    ) -> dict:
        """Commit a local raw file (or reuse the cataloged copy) and register its scan."""
        file_path = Path(path)
        if scan_time is None:
            raise StoreError(
                f"Cannot acquire '{file_path.name}': the source provided no scan_time — "
                "wall-clock substitution and filename re-parsing are forbidden"
            )
        existing = self._collection.catalog.find_by_source_uri(source_uri)
        if existing is not None:
            return self._register(
                existing["artifact_id"],
                existing["scan_id"],
                scan_time,
                existing["original_filename"],
            )

        data = file_path.read_bytes()
        scan_id = scan_id_from_bytes(data)
        handle = self._objects.begin(suffix=file_path.suffix)
        handle.staging_path.write_bytes(data)
        record = self._objects.commit(
            handle,
            ArtifactMeta(
                artifact_type="raw_volume",
                producer="acquisition",
                run_id=self._run_id,
                scan_id=scan_id,
                observation_time=scan_time,
                original_filename=file_path.name,
                source_uri=source_uri,
            ),
        )
        return self._register(record.artifact_id, scan_id, scan_time, file_path.name)

    def acquire_existing(self, source_uri: str, *, scan_time: datetime | None) -> dict:
        """Register this run against an already-stored source (skip-if-downloaded)."""
        existing = self._collection.catalog.find_by_source_uri(source_uri)
        if existing is None:
            raise StoreError(f"No stored object for source '{source_uri}' — download it first")
        if scan_time is None:
            raise StoreError(
                f"Cannot acquire '{existing['original_filename']}': the source provided "
                "no scan_time — wall-clock substitution is forbidden"
            )
        return self._register(
            existing["artifact_id"],
            existing["scan_id"],
            scan_time,
            existing["original_filename"],
        )

    def _register(
        self, artifact_id: str, scan_id: str, scan_time: datetime, source_file_name: str
    ) -> dict:
        self._collection.catalog.register_scan(
            ScanRecord(
                run_id=self._run_id,
                scan_id=scan_id,
                scan_time=scan_time,
                source_file_name=source_file_name,
            )
        )
        return {
            "artifact_id": artifact_id,
            "scan_id": scan_id,
            "scan_time": scan_time,
            "queued_at": time.time(),
        }
