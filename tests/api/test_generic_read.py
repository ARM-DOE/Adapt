# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Generic, catalog-driven read API.

A table written through the OutputRouter (which registers its schema in
``module_schemas``) must be discoverable and readable with zero client
changes; artifacts are discoverable through the ``items`` catalog table.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd
import pytest
import xarray as xr

from adapt.api.client import RepositoryClient
from adapt.contracts import PersistenceMeta, RegisterFileArtifact, SqliteTable
from adapt.persistence.output_router import OutputRouter
from adapt.persistence.repository import DataRepository

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


@dataclass
class FakeModule:
    name: str = "fake"
    persistence: tuple = field(default_factory=tuple)


@pytest.fixture
def written_repo(tmp_path):
    """A repository with one router-written extension table and one artifact."""
    from adapt.persistence.registry import RepositoryRegistry

    RepositoryRegistry._instance = None
    repo = DataRepository(run_id="genericread1", base_dir=tmp_path, radar="KTST")
    meta = PersistenceMeta(
        scan_time=SCAN_TIME, run_id=repo.run_id, source_file="src", dataset_id="KTST"
    )

    nc = tmp_path / "grid.nc"
    xr.Dataset({"reflectivity": ("x", [1.0])}).to_netcdf(nc)

    mod = FakeModule(
        persistence=(
            SqliteTable(
                key="rows",
                table="synthetic_ext",
                primary_key=("run_id", "cell_uid"),
                index_columns=("cell_uid",),
            ),
            RegisterFileArtifact(key="nc_path", product_type="gridded3d", producer="ingest"),
        )
    )
    rows = pd.DataFrame({"run_id": [repo.run_id] * 2, "cell_uid": ["U1", "U2"], "v": [1.0, 2.0]})
    OutputRouter(repo).persist([mod], {"rows": rows, "nc_path": str(nc)}, meta)
    repo.close()
    RepositoryRegistry._instance = None
    yield tmp_path
    RepositoryRegistry._instance = None


@pytest.fixture
def written_client(written_repo):
    c = RepositoryClient(written_repo)
    yield c
    c.close()


class TestTables:
    def test_lists_core_and_router_written_extension_tables(self, written_client):
        df = written_client.tables("KTST")
        names = set(df["table_name"])
        assert {"cells_by_scan", "cell_events", "cell_tracks"} <= names
        assert "synthetic_ext" in names
        kinds = dict(zip(df["table_name"], df["kind"], strict=True))
        assert kinds["synthetic_ext"] == "extension"
        assert kinds["cells_by_scan"] == "core"

    def test_core_only_when_no_extensions(self, client):
        df = client.tables()
        assert set(df["kind"]) == {"core"}


class TestTable:
    def test_reads_router_written_table(self, written_client):
        df = written_client.table("synthetic_ext", radar="KTST")
        assert sorted(df["cell_uid"]) == ["U1", "U2"]

    def test_equality_filters(self, written_client):
        df = written_client.table(
            "synthetic_ext", radar="KTST", run_id="genericread1", filters={"cell_uid": "U2"}
        )
        assert df["v"].tolist() == [2.0]

    def test_unknown_table_raises_with_available_names(self, written_client):
        with pytest.raises(ValueError, match="synthetic_ext"):
            written_client.table("nonexistent", radar="KTST")

    def test_invalid_filter_column_raises(self, written_client):
        with pytest.raises(ValueError, match="Invalid filter column"):
            written_client.table("synthetic_ext", radar="KTST", filters={"x; DROP TABLE items": 1})


class TestArtifacts:
    def test_lists_registered_artifacts(self, written_client):
        df = written_client.artifacts(product_type="gridded3d", radar="KTST")
        assert len(df) == 1
        assert df.iloc[0]["item_type"] == "gridded3d"

    def test_time_window_filters(self, written_client):
        before = written_client.artifacts(radar="KTST", end=datetime(2026, 6, 1, 11, 0, tzinfo=UTC))
        assert before.empty


class TestOpenArtifact:
    def test_opens_netcdf_artifact(self, written_client):
        item = written_client.artifacts(product_type="gridded3d", radar="KTST").iloc[0]
        ds = written_client.open_artifact(item["item_id"], radar="KTST")
        assert "reflectivity" in ds.data_vars
        ds.close()

    def test_unknown_item_raises(self, written_client):
        with pytest.raises(ValueError, match="Unknown artifact"):
            written_client.open_artifact("no-such-id", radar="KTST")

    def test_missing_file_raises_not_none(self, written_client, written_repo):
        item = written_client.artifacts(product_type="gridded3d", radar="KTST").iloc[0]
        (written_repo / "KTST" / item["file_path"]).unlink(missing_ok=False)
        with pytest.raises(FileNotFoundError):
            written_client.open_artifact(item["item_id"], radar="KTST")
