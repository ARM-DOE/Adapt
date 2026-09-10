# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Immutable object store for one collection's ``objects/`` directory.

Write protocol: :meth:`ObjectStore.begin` allocates the uuid4 artifact_id
BEFORE any bytes exist and hands back a staging path inside store-owned
``objects/.staging/``. Nothing is visible or cataloged until :meth:`commit`,
which checksums the staged bytes, atomically publishes them via
``os.replace``, and registers the full catalog row. :meth:`abort` removes the
staging file and leaves no trace.
"""

import hashlib
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from adapt.persistence.collection_catalog import ArtifactRecord, Catalog
from adapt.persistence.errors import StoreError
from adapt.utils.time import to_scan_iso

__all__ = ["ArtifactMeta", "ObjectStore", "ObjectWriteHandle"]

_HASH_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class ArtifactMeta:
    """Caller-supplied metadata for one object; the store computes the rest."""

    artifact_type: str
    producer: str
    run_id: str
    scan_id: str | None
    observation_time: datetime | None
    original_filename: str | None = None
    source_uri: str | None = None


@dataclass(frozen=True)
class ObjectWriteHandle:
    """One in-flight object write: id allocated, bytes staged, not yet visible."""

    artifact_id: str
    suffix: str
    staging_path: Path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


class ObjectStore:
    """Atomic, catalog-registered writes into one collection's ``objects/``."""

    def __init__(self, objects_dir: str | Path, catalog: Catalog) -> None:
        self.objects_dir = Path(objects_dir)
        self._catalog = catalog
        self._staging_dir = self.objects_dir / ".staging"

    def begin(self, *, suffix: str) -> ObjectWriteHandle:
        """Allocate an artifact_id and staging path; write bytes there, then commit."""
        self._staging_dir.mkdir(exist_ok=True)
        artifact_id = uuid.uuid4().hex
        return ObjectWriteHandle(
            artifact_id=artifact_id,
            suffix=suffix,
            staging_path=self._staging_dir / f"{artifact_id}{suffix}",
        )

    def commit(
        self, handle: ObjectWriteHandle, meta: ArtifactMeta, parents: Sequence[str] = ()
    ) -> ArtifactRecord:
        """Publish the staged bytes atomically, register the row and lineage edges."""
        if not handle.staging_path.exists():
            raise StoreError(
                f"No staged object for artifact '{handle.artifact_id}' "
                f"(missing {handle.staging_path}); begin/write before commit, "
                "and commit only once"
            )
        object_name = f"{handle.artifact_id}{handle.suffix}"
        final_path = self.objects_dir / object_name
        if final_path.exists():
            raise StoreError(f"Object '{object_name}' already exists; objects are immutable")
        for parent in parents:
            if self._catalog.get_artifact(parent) is None:
                raise StoreError(
                    f"Cannot commit '{handle.artifact_id}': lineage parent "
                    f"'{parent}' is not cataloged"
                )

        record = ArtifactRecord(
            artifact_id=handle.artifact_id,
            artifact_type=meta.artifact_type,
            producer=meta.producer,
            run_id=meta.run_id,
            scan_id=meta.scan_id,
            observation_time=(
                to_scan_iso(meta.observation_time) if meta.observation_time is not None else None
            ),
            original_filename=meta.original_filename,
            source_uri=meta.source_uri,
            object_name=object_name,
            checksum_sha256=_sha256_file(handle.staging_path),
            size_bytes=handle.staging_path.stat().st_size,
        )
        os.replace(handle.staging_path, final_path)
        self._catalog.register_artifact(record)
        for parent in parents:
            self._catalog.add_lineage(record.artifact_id, parent)
        return record

    def abort(self, handle: ObjectWriteHandle) -> None:
        """Discard the staged bytes; nothing was published or cataloged."""
        handle.staging_path.unlink(missing_ok=True)
