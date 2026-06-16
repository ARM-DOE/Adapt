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
from pathlib import Path

from adapt.contracts import (
    NetcdfArtifact,
    ParquetArtifact,
    PersistenceMeta,
    PersistenceSpec,
    RegisterFileArtifact,
    SqliteTable,
    TrackTablesWrite,
)
from adapt.persistence.module_output import ModuleOutputWriter
from adapt.persistence.repository import DataRepository
from adapt.persistence.track_store import TrackStore

logger = logging.getLogger(__name__)


class OutputRouter:
    """Persist every output declared by the given modules. No science names."""

    def __init__(self, repository: DataRepository) -> None:
        self._repo = repository

    def persist(self, modules: Iterable, result: dict, meta: PersistenceMeta) -> None:
        for module in modules:
            for spec in module.persistence:
                self._dispatch(module.name, spec, result, meta)

    @staticmethod
    def _require_scan_time(module_name: str, spec, meta: PersistenceMeta) -> datetime:
        if meta.scan_time is None:
            raise ValueError(
                f"{module_name}: {type(spec).__name__} needs scan_time but it is None — "
                "refusing to persist with unknown observation time "
                "(wall-clock substitution is forbidden)"
            )
        return meta.scan_time

    def _dispatch(
        self, module_name: str, spec: PersistenceSpec, result: dict, meta: PersistenceMeta
    ) -> None:
        match spec:
            case RegisterFileArtifact():
                if spec.key not in result:
                    return
                scan_time = self._require_scan_time(module_name, spec, meta)
                path = result[spec.key]
                if not Path(path).exists():
                    raise FileNotFoundError(
                        f"{module_name}: declared artifact '{spec.key}' = {path} does not exist"
                    )
                self._repo.register_artifact(
                    product_type=spec.product_type,
                    file_path=path,
                    scan_time=scan_time,
                    producer=spec.producer,
                )
            case NetcdfArtifact():
                if spec.key not in result:
                    return
                scan_time = self._require_scan_time(module_name, spec, meta)
                ds = result[spec.key]
                # attrs key stays "radar" — stored-artifact format unchanged
                ds.attrs.update(
                    {
                        "source": meta.source_file,
                        "radar": meta.dataset_id,
                        "description": spec.description,
                    }
                )
                self._repo.write_netcdf(
                    ds=ds,
                    product_type=spec.product_type,
                    scan_time=scan_time,
                    producer=spec.producer,
                    parent_ids=[],
                    metadata={"components": list(ds.data_vars)},
                    filename_stem=Path(meta.source_file).stem,
                )
            case ParquetArtifact():
                df = result.get(spec.key)
                if df is None or df.empty:
                    return
                scan_time = self._require_scan_time(module_name, spec, meta)
                self._repo.write_parquet(
                    df=df,
                    product_type=spec.product_type,
                    scan_time=scan_time,
                    producer=spec.producer,
                )
            case TrackTablesWrite():
                if spec.tracked_key not in result:
                    return
                tracked = result[spec.tracked_key]
                if tracked.empty:
                    return
                scan_time = self._require_scan_time(module_name, spec, meta)
                missing = [
                    k
                    for k in (spec.events_key, spec.stats_key, spec.adjacency_key)
                    if k not in result
                ]
                if missing:
                    raise KeyError(
                        f"{module_name}: TrackTablesWrite requires context keys {missing} "
                        f"alongside '{spec.tracked_key}'"
                    )
                TrackStore(self._repo.catalog.db_path).write_scan(
                    run_id=meta.run_id,
                    scan_time=scan_time,
                    cell_stats_df=result[spec.stats_key],
                    tracked_cells_df=tracked,
                    cell_events_df=result[spec.events_key],
                    cell_adjacency_df=result[spec.adjacency_key],
                )
            case SqliteTable():
                df = result.get(spec.key)
                if df is None or df.empty:
                    return
                ModuleOutputWriter(self._repo.catalog.db_path, spec).write(df)
            case _:
                raise TypeError(f"{module_name}: unknown persistence spec {type(spec).__name__}")
