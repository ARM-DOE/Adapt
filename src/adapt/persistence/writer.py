# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Repository writer — module-facing interface for persisting pipeline outputs.

Thin facade over DataRepository. Modules call RepositoryWriter methods
instead of accessing DataRepository directly, keeping module code independent
from storage implementation details.
"""

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import xarray as xr

    from adapt.persistence.repository import DataRepository


class RepositoryWriter:
    """Write pipeline outputs to the DataRepository.

    Parameters
    ----------
    repository : DataRepository
        The underlying storage backend.
    """

    def __init__(self, repository: "DataRepository") -> None:
        self.repository = repository

    def write_analysis(
        self,
        df: pd.DataFrame,
        scan_time: datetime,
        producer: str,
        parent_ids: list[str] | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Persist cell analysis DataFrame as a Parquet artifact. Returns artifact ID."""
        return self.repository.write_analysis2d_parquet(
            df=df,
            scan_time=scan_time,
            producer=producer,
            parent_ids=parent_ids or [],
            metadata=metadata or {},
        )

    def write_netcdf(
        self,
        ds: "xr.Dataset",
        path: Path,
        scan_time: datetime,
        producer: str,
        parent_ids: list[str] | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Persist an xarray Dataset as a NetCDF artifact. Returns artifact ID."""
        return self.repository.write_netcdf(
            ds=ds,
            path=path,
            scan_time=scan_time,
            producer=producer,
            parent_ids=parent_ids or [],
            metadata=metadata or {},
        )
