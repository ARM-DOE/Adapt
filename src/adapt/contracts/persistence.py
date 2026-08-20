# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Persistence spec types. Modules declare HOW each output is persisted;
the persistence layer routes mechanically. Types only — zero logic."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RegisterFileArtifact:
    """Register an already-written file (context key holds its path) in the catalog."""

    key: str
    product_type: str  # catalog vocabulary, e.g. "gridded3d"
    producer: str


@dataclass(frozen=True)
class NetcdfArtifact:
    """Write an xarray.Dataset (context key) as a NetCDF artifact."""

    key: str
    product_type: str  # e.g. "segmentation2d"
    producer: str
    description: str  # stamped into ds.attrs["description"]


@dataclass(frozen=True)
class ParquetArtifact:
    """Append a DataFrame (context key) to the run's Parquet store for this product type."""

    key: str
    product_type: str  # e.g. "analysis2d"
    producer: str


@dataclass(frozen=True)
class TrackTablesWrite:
    """Joint write of the core tracking tables; consumes four context keys.

    DEBT: encodes tracking science that lives in TrackStore.write_scan.
    Follow-up ticket: tracking emits final row DataFrames so this decomposes
    into plain SqliteTable specs.
    """

    tracked_key: str
    events_key: str
    stats_key: str
    adjacency_key: str


@dataclass(frozen=True)
class ScanRecord:
    """One scan's registration in the catalog; built by the runtime after persist.

    ``scan_id`` is the content-derived identity (the join key); ``scan_time`` is
    canonical UTC ordering/display metadata. ``start_time``/``end_time`` are
    per-source coverage metadata — not every source has them.
    """

    run_id: str
    scan_id: str
    scan_time: datetime  # tz-aware UTC
    source_file_name: str
    start_time: datetime | None = None
    end_time: datetime | None = None


@dataclass(frozen=True)
class SqliteTable:
    """Upsert a DataFrame (context key) into an extension table in catalog.db."""

    key: str
    table: str
    primary_key: tuple[str, ...]
    index_columns: tuple[str, ...] = ()


PersistenceSpec = (
    RegisterFileArtifact | NetcdfArtifact | ParquetArtifact | TrackTablesWrite | SqliteTable
)


@dataclass(frozen=True)
class PersistenceMeta:
    """Run-scoped metadata the persistence router needs; built by the runtime per scan.

    ``scan_id`` is the content-derived scan identity minted at the source
    boundary — the single join key across tracking tables, catalog rows, and
    artifacts. It may be None only for run-level persists that aggregate many
    scans; any per-scan write raises on a missing scan_id.

    ``scan_time`` may be None only for run-level persists whose specs do not
    stamp a time (SqliteTable rows carry their own); any time-stamped artifact
    write raises on a missing scan_time — wall-clock substitution is forbidden.
    """

    scan_time: datetime | None  # tz-aware UTC (ordering/display metadata)
    scan_id: str | None  # content-derived scan identity (join key)
    run_id: str
    source_file: str  # source scan path -> filename_stem, ds.attrs["source"]
    dataset_id: str  # domain-neutral dataset identity; value = radar ID today
