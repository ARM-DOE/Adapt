# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Discovery + provenance catalog for one collection.

Backs ``collections/{id}/catalog.db`` (schema ``collection_catalog.sql``).
Science rows live in products.db; this catalog answers which artifacts and
scans exist, which products each scan has, and where every object came from.
"""

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from adapt.contracts.persistence import ScanRecord
from adapt.persistence.errors import StoreError
from adapt.persistence.sqlite_store import SqliteStore
from adapt.utils.time import to_scan_iso

__all__ = ["REQUIRED_SCAN_PRODUCTS", "ArtifactRecord", "Catalog"]

# A scan is complete when exactly these products are linked: the gridded
# volume object, the segmentation/analysis NC object, and the cell_stats
# table. Auxiliary links (tracking, volume stats, adjacency, ...) never
# complete a scan.
REQUIRED_SCAN_PRODUCTS = ("gridded3d", "segmentation2d", "cell_stats")


@dataclass(frozen=True)
class ArtifactRecord:
    """One complete artifacts row; built by the ObjectStore at commit."""

    artifact_id: str
    artifact_type: str
    producer: str
    run_id: str
    scan_id: str | None
    observation_time: str | None  # canonical scan-ISO string
    original_filename: str | None
    source_uri: str | None
    object_name: str
    checksum_sha256: str
    size_bytes: int


class Catalog(SqliteStore):
    """Catalog over ``{collection_dir}/catalog.db``."""

    def __init__(self, collection_dir: str | Path) -> None:
        self.collection_dir = Path(collection_dir)
        super().__init__(self.collection_dir / "catalog.db", "collection_catalog.sql")

    def register_artifact(self, record: ArtifactRecord) -> None:
        """Insert one artifacts row; duplicate artifact_id or object_name raise."""
        now = datetime.now(UTC).isoformat()
        conn = self._get_connection()
        with self._lock:
            conn.execute(
                "INSERT INTO artifacts (artifact_id, artifact_type, producer, run_id, "
                " scan_id, observation_time, original_filename, source_uri, object_name, "
                " checksum_sha256, size_bytes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.artifact_id,
                    record.artifact_type,
                    record.producer,
                    record.run_id,
                    record.scan_id,
                    record.observation_time,
                    record.original_filename,
                    record.source_uri,
                    record.object_name,
                    record.checksum_sha256,
                    record.size_bytes,
                    now,
                ),
            )
            conn.commit()

    def get_artifact(self, artifact_id: str) -> dict | None:
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM artifacts WHERE artifact_id = ?", (artifact_id,)
            ).fetchone()
        return dict(row) if row else None

    def find_artifact(self, run_id: str, scan_id: str, artifact_type: str) -> dict | None:
        """Newest artifact of one type for a scan (re-runs replace, newest wins)."""
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM artifacts WHERE run_id = ? AND scan_id = ? "
                "AND artifact_type = ? ORDER BY created_at DESC, artifact_id DESC LIMIT 1",
                (run_id, scan_id, artifact_type),
            ).fetchone()
        return dict(row) if row else None

    def find_artifact_by_scan(self, scan_id: str, artifact_type: str) -> dict | None:
        """Newest artifact of one type for a scan across ALL runs.

        Raw volumes are content-identified (same bytes → same scan_id), so a
        later run reuses the object an earlier run committed.
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM artifacts WHERE scan_id = ? AND artifact_type = ? "
                "ORDER BY created_at DESC, artifact_id DESC LIMIT 1",
                (scan_id, artifact_type),
            ).fetchone()
        return dict(row) if row else None

    def find_by_source_uri(self, source_uri: str) -> dict | None:
        """Newest artifact acquired from one source URI (skip-if-downloaded lookup)."""
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM artifacts WHERE source_uri = ? "
                "ORDER BY created_at DESC, artifact_id DESC LIMIT 1",
                (source_uri,),
            ).fetchone()
        return dict(row) if row else None

    # -- table schema mirror -----------------------------------------------------

    def register_table_schema(self, snapshot: dict) -> None:
        """Mirror one frozen table-schema snapshot (written by the SchemaLedger)."""
        conn = self._get_connection()
        with self._lock:
            try:
                conn.execute(
                    "INSERT INTO table_schemas (table_name, owner_module, granularity, "
                    " primary_key, index_columns, columns_json, frozen_at) "
                    "VALUES (:table_name, :owner_module, :granularity, :primary_key, "
                    " :index_columns, :columns_json, :frozen_at)",
                    snapshot,
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(
                    f"Table '{snapshot['table_name']}' is already frozen in the catalog"
                ) from exc
            conn.commit()

    # -- lineage ---------------------------------------------------------------

    def add_lineage(
        self, child_artifact_id: str, parent_artifact_id: str, relationship: str = "derived_from"
    ) -> None:
        """Record one provenance edge; both artifacts must exist, edges are unique."""
        conn = self._get_connection()
        with self._lock:
            for artifact_id, role in (
                (child_artifact_id, "child"),
                (parent_artifact_id, "parent"),
            ):
                known = conn.execute(
                    "SELECT 1 FROM artifacts WHERE artifact_id = ?", (artifact_id,)
                ).fetchone()
                if known is None:
                    raise StoreError(
                        f"Cannot add lineage: {role} artifact '{artifact_id}' is not cataloged"
                    )
            try:
                conn.execute(
                    "INSERT INTO artifact_lineage "
                    "(child_artifact_id, parent_artifact_id, relationship) VALUES (?, ?, ?)",
                    (child_artifact_id, parent_artifact_id, relationship),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(
                    f"Duplicate lineage edge {child_artifact_id} -> {parent_artifact_id} "
                    f"({relationship})"
                ) from exc
            conn.commit()

    # -- scans + completeness ----------------------------------------------------

    def register_scan(self, record: ScanRecord) -> None:
        """Upsert one scan keyed by (run_id, scan_id); starts ``pending``.

        ``scan_time`` is stored in the canonical format (``to_scan_iso``);
        coverage times are optional per-source metadata kept at full precision.
        Two different scans sharing one nominal second within a run raise.
        """
        now = datetime.now(UTC).isoformat()
        conn = self._get_connection()
        with self._lock:
            try:
                conn.execute(
                    "INSERT INTO scans (run_id, scan_id, scan_time, scan_date, start_time, "
                    " end_time, source_file_name, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(run_id, scan_id) DO UPDATE SET "
                    " scan_time=excluded.scan_time, scan_date=excluded.scan_date, "
                    " start_time=excluded.start_time, end_time=excluded.end_time, "
                    " source_file_name=excluded.source_file_name, "
                    " updated_at=excluded.updated_at",
                    (
                        record.run_id,
                        record.scan_id,
                        to_scan_iso(record.scan_time),
                        record.scan_time.strftime("%Y%m%d"),
                        record.start_time.isoformat() if record.start_time else None,
                        record.end_time.isoformat() if record.end_time else None,
                        record.source_file_name,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(
                    f"Scan '{record.scan_id}' collides on scan_time "
                    f"{to_scan_iso(record.scan_time)} within run '{record.run_id}' — "
                    "two different scans share one nominal second (duplicate download?)"
                ) from exc
            conn.commit()

    def get_scan(self, run_id: str, scan_id: str) -> dict | None:
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM scans WHERE run_id = ? AND scan_id = ?", (run_id, scan_id)
            ).fetchone()
        return dict(row) if row else None

    def link_scan_product(
        self,
        run_id: str,
        scan_id: str,
        product: str,
        *,
        artifact_id: str | None = None,
        table_name: str | None = None,
    ) -> None:
        """Link one product to a scan; re-linking replaces (module re-run semantics).

        Exactly one of ``artifact_id``/``table_name`` identifies where the
        product lives. Completeness is recomputed after every link.
        """
        if (artifact_id is None) == (table_name is None):
            raise ValueError("link_scan_product takes exactly one of artifact_id or table_name")
        kind = "artifact" if artifact_id is not None else "table"
        now = datetime.now(UTC).isoformat()
        conn = self._get_connection()
        with self._lock:
            known = conn.execute(
                "SELECT 1 FROM scans WHERE run_id = ? AND scan_id = ?", (run_id, scan_id)
            ).fetchone()
            if known is None:
                raise StoreError(
                    f"Cannot link product '{product}': scan '{scan_id}' is not "
                    f"registered for run '{run_id}'"
                )
            if artifact_id is not None:
                cataloged = conn.execute(
                    "SELECT 1 FROM artifacts WHERE artifact_id = ?", (artifact_id,)
                ).fetchone()
                if cataloged is None:
                    raise StoreError(
                        f"Cannot link product '{product}': artifact '{artifact_id}' "
                        "is not cataloged"
                    )
            conn.execute(
                "INSERT INTO scan_products (run_id, scan_id, product, kind, artifact_id, "
                " table_name, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(run_id, scan_id, product) DO UPDATE SET "
                " kind=excluded.kind, artifact_id=excluded.artifact_id, "
                " table_name=excluded.table_name, created_at=excluded.created_at",
                (run_id, scan_id, product, kind, artifact_id, table_name, now),
            )
            linked = {
                row["product"]
                for row in conn.execute(
                    "SELECT product FROM scan_products WHERE run_id = ? AND scan_id = ?",
                    (run_id, scan_id),
                ).fetchall()
            }
            if set(REQUIRED_SCAN_PRODUCTS) <= linked:
                conn.execute(
                    "UPDATE scans SET status = 'complete', updated_at = ? "
                    "WHERE run_id = ? AND scan_id = ?",
                    (now, run_id, scan_id),
                )
            conn.commit()

    def mark_scan_failed(self, run_id: str, scan_id: str) -> None:
        """Record a scan-processing failure; a later successful re-run re-completes it."""
        conn = self._get_connection()
        with self._lock:
            updated = conn.execute(
                "UPDATE scans SET status = 'failed', updated_at = ? "
                "WHERE run_id = ? AND scan_id = ?",
                (datetime.now(UTC).isoformat(), run_id, scan_id),
            )
            if updated.rowcount == 0:
                raise StoreError(
                    f"Cannot mark scan '{scan_id}' failed: not registered for run '{run_id}'"
                )
            conn.commit()

    def list_artifacts(
        self, run_id: str | None = None, artifact_type: str | None = None
    ) -> list[dict]:
        """Artifacts filtered by run and/or type, ordered by observation time."""
        clauses, params = [], []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if artifact_type is not None:
            clauses.append("artifact_type = ?")
            params.append(artifact_type)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute(
                f"SELECT * FROM artifacts {where} ORDER BY observation_time, artifact_id",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def scan_products(self, run_id: str, scan_id: str) -> dict[str, dict]:
        """All product links for one scan, keyed by product name."""
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM scan_products WHERE run_id = ? AND scan_id = ?",
                (run_id, scan_id),
            ).fetchall()
        return {row["product"]: dict(row) for row in rows}

    def scan_is_complete(self, run_id: str, scan_id: str) -> bool:
        scan = self.get_scan(run_id, scan_id)
        return scan is not None and scan["status"] == "complete"

    def complete_scans(self, run_id: str) -> list[dict]:
        """Complete scans of one run, time-ordered."""
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM scans WHERE run_id = ? AND status = 'complete' ORDER BY scan_time",
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def parents_of(self, artifact_id: str) -> list[str]:
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute(
                "SELECT parent_artifact_id FROM artifact_lineage "
                "WHERE child_artifact_id = ? ORDER BY parent_artifact_id",
                (artifact_id,),
            ).fetchall()
        return [row["parent_artifact_id"] for row in rows]

    def children_of(self, artifact_id: str) -> list[str]:
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute(
                "SELECT child_artifact_id FROM artifact_lineage "
                "WHERE parent_artifact_id = ? ORDER BY child_artifact_id",
                (artifact_id,),
            ).fetchall()
        return [row["child_artifact_id"] for row in rows]
