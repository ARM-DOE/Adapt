# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Writer for the analysis-only decision log (``decisions.db``).

Each decision bundle (``adapt.contracts.decisions``) maps to fixed tables whose
static DDL lives in ``configuration/schemas/collection_decisions.sql``; a row is
the record's fields plus the run/scan stamps from ``PersistenceMeta``. Writes
replace on the primary key, so a post-hoc re-run under the same run id
replaces its own rows. One connection per scan: use as a context manager.
"""

import sqlite3
from dataclasses import asdict

from adapt.contracts import PersistenceMeta, SegmentationDecisions, TrackingDecisions
from adapt.persistence.sqlite_store import SqliteStore
from adapt.utils.time import to_scan_iso

__all__ = ["DecisionStore"]

# Tables whose rows have no natural key: keyed by their order within the scan.
_SEQUENCED = frozenset({"segmentation_seeds", "tracking_lineage"})


class DecisionStore(SqliteStore):
    """One-scan writer for ``decisions.db``."""

    def __init__(self, db_path) -> None:
        super().__init__(db_path, "collection_decisions.sql")

    def __enter__(self) -> "DecisionStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def write(
        self, decisions: TrackingDecisions | SegmentationDecisions, meta: PersistenceMeta
    ) -> None:
        """Write one scan's bundle; every table of the bundle is written, even if empty."""
        if meta.scan_id is None or meta.scan_time is None:
            raise ValueError("DecisionStore.write needs scan_id and scan_time")
        stamp = {
            "run_id": meta.run_id,
            "scan_id": meta.scan_id,
            "scan_time": to_scan_iso(meta.scan_time),
        }
        tables: tuple[tuple[str, tuple], ...]
        match decisions:
            case TrackingDecisions():
                tables = (
                    ("tracking_scans", (decisions.scan,)),
                    ("tracking_cells", decisions.cells),
                    ("tracking_pairs", decisions.pairs),
                    ("tracking_lineage", decisions.lineage),
                    ("tracking_identity", decisions.identity),
                    ("tracking_latent", decisions.latent),
                )
            case SegmentationDecisions():
                tables = (
                    ("segmentation_frames", (decisions.frame,)),
                    ("segmentation_seeds", decisions.seeds),
                    ("segmentation_components", decisions.components),
                )
            case _:
                raise TypeError(f"DecisionStore cannot write {type(decisions).__name__}")

        conn = self._get_connection()
        for table, records in tables:
            rows = [{**stamp, **asdict(record)} for record in records]
            if table in _SEQUENCED:
                for seq, row in enumerate(rows):
                    row["seq"] = seq
            self._replace(conn, table, rows)
        conn.commit()

    @staticmethod
    def _replace(conn: sqlite3.Connection, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        columns = list(rows[0])
        placeholders = ", ".join("?" for _ in columns)
        conn.executemany(
            f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(row[c] for c in columns) for row in rows],
        )
