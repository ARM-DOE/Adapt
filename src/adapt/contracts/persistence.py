# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Persistence spec types. Modules declare HOW each output is persisted;
the persistence layer routes mechanically. Types only — zero logic."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class NetcdfArtifact:
    """Write an xarray.Dataset (context key) as a NetCDF artifact."""

    key: str
    product_type: str  # e.g. "segmentation2d"
    producer: str
    description: str  # stamped into ds.attrs["description"]


@dataclass(frozen=True)
class TrackTablesWrite:
    """Joint write of the core tracking tables; consumes four context keys.

    DEBT: encodes tracking science that lives in TrackStore.write_scan.
    Follow-up ticket: tracking emits final row DataFrames so this decomposes
    into plain ProductTableWrite specs.
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
class ProductTableWrite:
    """Upsert a DataFrame (context key) into a module-owned products.db table.

    The schema freezes on the first non-empty frame (SchemaLedger); the table's
    granularity derives from the primary key: ``valid_time`` in the key marks a
    time-granular product, otherwise ``scan_id`` marks scan granularity,
    otherwise the table is run-granular.
    """

    key: str
    table: str
    primary_key: tuple[str, ...]
    index_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class DecisionTableWrite:
    """Write a decision bundle (context key) to the collection's analysis-only
    decisions.db. Never a scan product: no catalog link, no API exposure."""

    key: str


PersistenceSpec = NetcdfArtifact | TrackTablesWrite | ProductTableWrite | DecisionTableWrite


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
    collection_id: str  # collection (radar/site data domain) identity; value = radar ID today
