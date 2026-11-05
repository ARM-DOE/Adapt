# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Mechanical persistence of module-declared outputs.

The router knows spec TYPES, never module key names. Adding a module output
means declaring a spec on the module — this file does not change.

Skip/raise rules: a key absent from the result means the module did not run
this scan (skip); an empty DataFrame means no rows (skip); everything else
that prevents a declared write is an error and raises.
"""

import logging
from collections.abc import Iterable
from datetime import datetime

from adapt.contracts import (
    DecisionTableWrite,
    NetcdfArtifact,
    PersistenceMeta,
    PersistenceSpec,
    ProductTableWrite,
    TrackTablesWrite,
)
from adapt.persistence.decision_store import DecisionStore
from adapt.persistence.errors import StoreError
from adapt.persistence.objects import ArtifactMeta, ObjectStore
from adapt.persistence.products import SchemaLedger, TableWriter
from adapt.persistence.store import Collection
from adapt.persistence.track_store import TrackStore
from adapt.utils.time import to_scan_iso

logger = logging.getLogger(__name__)


def _require_scan_time(module_name: str, spec, meta: PersistenceMeta) -> datetime:
    if meta.scan_time is None:
        raise ValueError(
            f"{module_name}: {type(spec).__name__} needs scan_time but it is None — "
            "refusing to persist with unknown observation time "
            "(wall-clock substitution is forbidden)"
        )
    return meta.scan_time


def _require_scan_id(module_name: str, spec, meta: PersistenceMeta) -> str:
    if meta.scan_id is None:
        raise ValueError(
            f"{module_name}: {type(spec).__name__} needs scan_id but it is None — "
            "refusing to persist rows without scan identity"
        )
    return meta.scan_id


class StoreOutputRouter:
    """Persist module-declared outputs into the store. No science names.

    Dispatch: NetcdfArtifact → immutable object + catalog row + lineage to the
    scan's raw volume + scan-product link; ProductTableWrite → products.db rows
    (frozen first-frame schema) + table link; TrackTablesWrite → TrackStore on
    products.db + auxiliary tracking link.
    """

    def __init__(self, collection: Collection) -> None:
        self._collection = collection
        self._objects = ObjectStore(collection.objects_dir, collection.catalog)
        self._ledger = SchemaLedger(collection.products_path, collection.catalog)

    def persist(self, modules: Iterable, result: dict, meta: PersistenceMeta) -> None:
        for module in modules:
            for spec in module.persistence:
                self._dispatch(module.name, spec, result, meta)

    def _dispatch(
        self, module_name: str, spec: PersistenceSpec, result: dict, meta: PersistenceMeta
    ) -> None:
        match spec:
            case NetcdfArtifact():
                self._write_netcdf(module_name, spec, result, meta)
            case ProductTableWrite():
                self._write_table(module_name, spec, result, meta)
            case TrackTablesWrite():
                self._write_tracking(module_name, spec, result, meta)
            case DecisionTableWrite():
                self._write_decisions(spec, result, meta)
            case _:
                raise TypeError(f"{module_name}: unknown persistence spec {type(spec).__name__}")

    def _write_decisions(
        self, spec: DecisionTableWrite, result: dict, meta: PersistenceMeta
    ) -> None:
        """Analysis-only decision log: never a scan product, so no catalog link.

        One connection per scan, as for the tracking tables.
        """
        if spec.key not in result:
            return
        with DecisionStore(self._collection.decisions_path) as store:
            store.write(result[spec.key], meta)

    def _write_netcdf(
        self, module_name: str, spec: NetcdfArtifact, result: dict, meta: PersistenceMeta
    ) -> None:
        if spec.key not in result:
            return
        scan_time = _require_scan_time(module_name, spec, meta)
        scan_id = _require_scan_id(module_name, spec, meta)
        raw = self._collection.catalog.find_artifact_by_scan(scan_id, "raw_volume")
        if raw is None:
            raise StoreError(
                f"{module_name}: no raw_volume artifact cataloged for scan '{scan_id}' "
                f"of run '{meta.run_id}' — the source must commit the raw object "
                "before products persist"
            )
        ds = result[spec.key]
        # attrs key stays "radar" — stored-artifact format unchanged. scan_id/
        # scan_time make the artifact self-describing: consumers key on attrs,
        # never on filenames or the time coordinate.
        ds.attrs.update(
            {
                "source": meta.source_file,
                "radar": meta.collection_id,
                "description": spec.description,
                "scan_id": scan_id,
                "scan_time": to_scan_iso(scan_time),
            }
        )
        handle = self._objects.begin(suffix=".nc")
        try:
            ds.to_netcdf(handle.staging_path)
        except Exception:
            self._objects.abort(handle)
            raise
        record = self._objects.commit(
            handle,
            ArtifactMeta(
                artifact_type=spec.product_type,
                producer=spec.producer,
                run_id=meta.run_id,
                scan_id=scan_id,
                observation_time=scan_time,
            ),
            parents=(raw["artifact_id"],),
        )
        self._collection.catalog.link_scan_product(
            meta.run_id, scan_id, spec.product_type, artifact_id=record.artifact_id
        )

    def _write_table(
        self, module_name: str, spec: ProductTableWrite, result: dict, meta: PersistenceMeta
    ) -> None:
        df = result.get(spec.key)
        if df is None or df.empty:
            return
        TableWriter(self._ledger, spec, owner_module=module_name).write(df, meta)
        if meta.scan_id is not None:
            self._collection.catalog.link_scan_product(
                meta.run_id, meta.scan_id, spec.table, table_name=spec.table
            )

    def _write_tracking(
        self, module_name: str, spec: TrackTablesWrite, result: dict, meta: PersistenceMeta
    ) -> None:
        if spec.tracked_key not in result:
            return
        tracked = result[spec.tracked_key]
        if tracked.empty:
            return
        scan_time = _require_scan_time(module_name, spec, meta)
        scan_id = _require_scan_id(module_name, spec, meta)
        missing = [
            k for k in (spec.events_key, spec.stats_key, spec.adjacency_key) if k not in result
        ]
        if missing:
            raise KeyError(
                f"{module_name}: TrackTablesWrite requires context keys {missing} "
                f"alongside '{spec.tracked_key}'"
            )
        # Context-managed: one write per scan; a store left open would leak a
        # products.db connection (plus WAL/SHM sidecars) every scan.
        with TrackStore(self._collection.products_path, ledger=self._ledger) as store:
            store.write_scan(
                run_id=meta.run_id,
                scan_time=scan_time,
                cell_stats_df=result[spec.stats_key],
                tracked_cells_df=tracked,
                cell_events_df=result[spec.events_key],
                cell_adjacency_df=result[spec.adjacency_key],
                scan_id=scan_id,
            )
        self._collection.catalog.link_scan_product(
            meta.run_id, scan_id, "tracking", table_name="cells_by_scan"
        )
