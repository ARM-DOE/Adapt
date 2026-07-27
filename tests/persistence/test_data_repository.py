"""Tests for DataRepository artifact management."""

import json
import re
import shutil
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.persistence import DataRepository, ProductType

# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def temp_base_dir():
    """Create temporary base directory."""
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def repository(temp_base_dir):
    """Create DataRepository instance."""
    repo = DataRepository(run_id="test1234", base_dir=temp_base_dir, radar="KDIX")
    yield repo
    repo.close()


@pytest.fixture
def sample_dataset():
    """Create a sample xarray Dataset."""
    return xr.Dataset(
        {
            "reflectivity": xr.DataArray(
                np.random.default_rng(0).standard_normal((10, 10)).astype(np.float32),
                dims=["y", "x"],
                coords={
                    "y": np.arange(10) * 1000.0,
                    "x": np.arange(10) * 1000.0,
                },
            )
        }
    )


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame."""
    return pd.DataFrame(
        {
            "cell_label": [1, 2, 3],
            "cell_area_sqkm": [100.0, 200.0, 150.0],
            "reflectivity_max": [45.5, 52.3, 48.1],
        }
    )


# =========================================================================
# Test: Catalog Initialization
# =========================================================================


class TestCatalogInitialization:
    """Test catalog database creation."""

    def test_directory_structure_created(self, repository, temp_base_dir):
        """All required directories should be created."""
        expected_dirs = [
            temp_base_dir / "KDIX" / "nexrad",
            temp_base_dir / "KDIX" / "gridnc",
            temp_base_dir / "KDIX" / "analysis",
            temp_base_dir / "KDIX" / "plots",
            temp_base_dir / "logs",
        ]
        for d in expected_dirs:
            assert d.exists(), f"Directory not created: {d}"

    def test_radar_catalog_created(self, repository, temp_base_dir):
        """RadarCatalog SQLite file should be created under the radar directory."""
        catalog_db = temp_base_dir / "KDIX" / "catalog.db"
        assert catalog_db.exists()

    def test_catalog_items_table_exists(self, repository):
        """RadarCatalog items table should exist."""
        conn = repository.catalog._get_connection()
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
        assert cursor.fetchone() is not None

    def test_run_registered_in_registry(self, repository):
        """Run should be registered in the root RepositoryRegistry."""
        runs = repository.registry.list_runs()
        assert not runs.empty
        assert repository.run_id in runs["run_id"].values


# =========================================================================
# Test: Artifact Registration
# =========================================================================


class TestArtifactRegistration:
    """Test artifact registration."""

    def test_register_artifact(self, repository, temp_base_dir):
        """Should register artifact and return ID."""
        file_path = temp_base_dir / "KDIX" / "nexrad" / "20260211" / "test.nc"
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        artifact_id = repository.register_artifact(
            product_type=ProductType.NEXRAD_RAW,
            file_path=file_path,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
            parent_ids=[],
            metadata={"test": True},
        )

        assert len(artifact_id) == 16

    def test_register_artifact_with_scan_time(self, repository, temp_base_dir):
        """Should register artifact with scan_time."""
        file_path = temp_base_dir / "KDIX" / "test_file.db"
        file_path.touch()

        artifact_id = repository.register_artifact(
            product_type=ProductType.CELLS_DB,
            file_path=file_path,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        assert len(artifact_id) == 16

    def test_query_artifacts(self, repository, temp_base_dir):
        """Should query registered artifacts."""
        file_path = temp_base_dir / "KDIX" / "test.nc"
        file_path.touch()

        artifact_id = repository.register_artifact(
            product_type=ProductType.GRIDDED_NC,
            file_path=file_path,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        results = repository.query(product_type=ProductType.GRIDDED_NC)
        assert len(results) == 1
        assert results[0]["artifact_id"] == artifact_id

    def test_query_by_time_range(self, repository, temp_base_dir):
        """Should filter by time range."""
        file1 = temp_base_dir / "KDIX" / "file1.nc"
        file2 = temp_base_dir / "KDIX" / "file2.nc"
        file1.touch()
        file2.touch()

        repository.register_artifact(
            product_type=ProductType.GRIDDED_NC,
            file_path=file1,
            scan_time=datetime(2026, 2, 11, 10, 0, 0, tzinfo=UTC),
            producer="test",
        )
        repository.register_artifact(
            product_type=ProductType.GRIDDED_NC,
            file_path=file2,
            scan_time=datetime(2026, 2, 11, 14, 0, 0, tzinfo=UTC),
            producer="test",
        )

        results = repository.query(
            product_type=ProductType.GRIDDED_NC,
            time_range=(
                datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
                datetime(2026, 2, 11, 16, 0, 0, tzinfo=UTC),
            ),
        )
        assert len(results) == 1

    def test_get_artifact(self, repository, temp_base_dir):
        """Should retrieve artifact by ID with expected fields."""
        file_path = temp_base_dir / "KDIX" / "test.nc"
        file_path.touch()

        artifact_id = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file_path,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="processor",
        )

        artifact = repository.get_artifact(artifact_id)
        assert artifact is not None
        assert artifact["product_type"] == ProductType.ANALYSIS_NC
        assert artifact["producer"] == "processor"

    def test_get_artifact_file_path_is_absolute(self, repository, temp_base_dir):
        """Returned artifact file_path should be absolute."""
        file_path = temp_base_dir / "KDIX" / "test.nc"
        file_path.touch()

        artifact_id = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file_path,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        artifact = repository.get_artifact(artifact_id)
        assert Path(artifact["file_path"]).is_absolute()


# =========================================================================
# Test: Write Operations
# =========================================================================


class TestWriteOperations:
    """Test atomic write operations."""

    def test_write_netcdf(self, repository, sample_dataset):
        """Should write NetCDF and register artifact."""
        artifact_id = repository.write_netcdf(
            ds=sample_dataset,
            product_type=ProductType.ANALYSIS_NC,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        artifact = repository.get_artifact(artifact_id)
        assert artifact is not None
        assert Path(artifact["file_path"]).exists()

        filename = Path(artifact["file_path"]).name
        assert "test1234" in filename  # run_id
        assert "analysis" in filename
        assert filename.endswith(".nc")

    def test_write_netcdf_replaces_existing_file(self, repository, sample_dataset):
        """Re-writing the same output path must overwrite it, not fail.

        The atomic write renames a temp file onto the destination. A rename that
        refuses an existing destination (the Windows semantics of os.rename, which
        shutil.move attempts first) makes every re-run of a scan fail.
        """
        scan_time = datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC)
        first = repository.write_netcdf(
            ds=sample_dataset,
            product_type=ProductType.GRIDDED_NC,
            scan_time=scan_time,
            producer="loader",
            filename_stem="same_stem",
        )
        path = Path(repository.get_artifact(first)["file_path"])

        updated = sample_dataset.copy(deep=True)
        updated["reflectivity"] += 100.0
        repository.write_netcdf(
            ds=updated,
            product_type=ProductType.GRIDDED_NC,
            scan_time=scan_time,
            producer="loader",
            filename_stem="same_stem",
        )

        with xr.open_dataset(path) as ds:
            assert float(ds["reflectivity"].min()) == pytest.approx(
                float(updated["reflectivity"].min()), abs=1e-3
            )

    def test_write_netcdf_gridded(self, repository, sample_dataset):
        """Should write gridded NetCDF with correct path."""
        artifact_id = repository.write_netcdf(
            ds=sample_dataset,
            product_type=ProductType.GRIDDED_NC,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="loader",
        )

        artifact = repository.get_artifact(artifact_id)
        file_path = Path(artifact["file_path"])

        assert "KDIX" in str(file_path)
        assert "gridnc" in str(file_path)
        assert "20260211" in str(file_path)
        assert "gridded" in file_path.name

    def test_write_netcdf_with_string_variable(self, repository):
        """A string data variable (e.g. cell_uid LUT) must survive write/read.

        zlib compression cannot be applied to variable-length strings, so the
        writer must skip compression for non-numeric variables.
        """
        ds = xr.Dataset(
            {
                "reflectivity": xr.DataArray(
                    np.random.default_rng(0).standard_normal((5, 5)).astype(np.float32),
                    dims=["y", "x"],
                ),
                "cell_uid": xr.DataArray(
                    np.array(["NONE", "abc", "def"], dtype=np.str_),
                    dims=["cell_label"],
                    coords={"cell_label": np.arange(3)},
                ),
            }
        )

        artifact_id = repository.write_netcdf(
            ds=ds,
            product_type=ProductType.ANALYSIS_NC,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        reopened = repository.open_dataset(artifact_id)
        try:
            assert list(reopened["cell_uid"].values.astype(str)) == ["NONE", "abc", "def"]
        finally:
            reopened.close()

    def test_open_dataset_uses_explicit_netcdf4_engine(
        self, repository, sample_dataset, monkeypatch
    ):
        """open_dataset must pin engine='netcdf4' instead of letting xarray sniff.

        Adapt writes its NetCDF with the netcdf4 engine, so reads should name it too.
        Without an explicit engine xarray probes every backend, and the h5netcdf probe
        makes libhdf5 splatter ``HDF5-DIAG`` blocks to stderr ("Not an HDF5 file") for
        every classic-format file. Pinning the engine skips the probe entirely.
        """
        from adapt.persistence import repository as repo_module

        artifact_id = repository.write_netcdf(
            ds=sample_dataset,
            product_type=ProductType.GRIDDED_NC,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="loader",
        )

        captured: dict = {}
        real_open = repo_module.xr.open_dataset

        def spy(path, *args, **kwargs):
            captured["engine"] = kwargs.get("engine")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(repo_module.xr, "open_dataset", spy)

        ds = repository.open_dataset(artifact_id)
        try:
            assert captured["engine"] == "netcdf4"
        finally:
            ds.close()

    def test_write_parquet(self, repository, sample_dataframe):
        """Should write Parquet and register artifact."""
        artifact_id = repository.write_parquet(
            df=sample_dataframe,
            product_type=ProductType.CELLS_PARQUET,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        artifact = repository.get_artifact(artifact_id)
        assert artifact is not None
        assert Path(artifact["file_path"]).exists()

        metadata = json.loads(artifact["metadata"])
        assert metadata["row_count"] == 3

    def test_get_or_create_cells_db(self, repository):
        """Should create cells database."""
        artifact_id = repository.get_or_create_cells_db(
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC), producer="processor"
        )

        artifact = repository.get_artifact(artifact_id)
        assert artifact is not None
        assert artifact["product_type"] == ProductType.CELLS_DB
        assert Path(artifact["file_path"]).exists()

    def test_get_or_create_cells_db_reuse(self, repository):
        """Should reuse existing cells database."""
        id1 = repository.get_or_create_cells_db(
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC), producer="processor"
        )
        id2 = repository.get_or_create_cells_db(
            scan_time=datetime(2026, 2, 11, 13, 0, 0, tzinfo=UTC), producer="processor"
        )

        assert id1 == id2

    def test_write_sqlite_table(self, repository, sample_dataframe):
        """Should write DataFrame to SQLite table."""
        db_artifact_id = repository.get_or_create_cells_db(
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC), producer="processor"
        )

        repository.write_sqlite_table(
            df=sample_dataframe, table_name="cells", artifact_id=db_artifact_id
        )

        artifact = repository.get_artifact(db_artifact_id)
        with sqlite3.connect(artifact["file_path"]) as conn:
            df_read = pd.read_sql("SELECT * FROM cells", conn)
            assert len(df_read) == 3


# =========================================================================
# Test: Data Access
# =========================================================================


class TestDataAccess:
    """Test data access operations."""

    def test_open_dataset(self, repository, sample_dataset):
        """Should open NetCDF as xarray Dataset."""
        artifact_id = repository.write_netcdf(
            ds=sample_dataset,
            product_type=ProductType.ANALYSIS_NC,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        opened_ds = repository.open_dataset(artifact_id)
        assert "reflectivity" in opened_ds.data_vars
        opened_ds.close()

    def test_open_dataset_invalid_type(self, repository, temp_base_dir):
        """Should raise error for non-NetCDF artifact."""
        file_path = temp_base_dir / "KDIX" / "test.db"
        file_path.touch()

        artifact_id = repository.register_artifact(
            product_type=ProductType.CELLS_DB,
            file_path=file_path,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        with pytest.raises(ValueError, match="Cannot open as dataset"):
            repository.open_dataset(artifact_id)

    def test_open_table_parquet(self, repository, sample_dataframe):
        """Should open Parquet as DataFrame."""
        artifact_id = repository.write_parquet(
            df=sample_dataframe,
            product_type=ProductType.CELLS_PARQUET,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        opened_df = repository.open_table(artifact_id)
        assert len(opened_df) == 3
        assert "cell_label" in opened_df.columns

    def test_open_table_sqlite(self, repository, sample_dataframe):
        """Should open SQLite table as DataFrame."""
        db_artifact_id = repository.get_or_create_cells_db(
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC), producer="processor"
        )
        repository.write_sqlite_table(
            df=sample_dataframe, table_name="cells", artifact_id=db_artifact_id
        )

        opened_df = repository.open_table(db_artifact_id, table_name="cells")
        assert len(opened_df) == 3

    def test_open_nonexistent_artifact(self, repository):
        """Should raise error for nonexistent artifact."""
        with pytest.raises(ValueError, match="Artifact not found"):
            repository.open_dataset("nonexistent")


# =========================================================================
# Test: Lifecycle
# =========================================================================


class TestLifecycle:
    """Test repository lifecycle."""

    def test_finalize_run(self, repository):
        """Should mark run as complete in registry."""
        repository.finalize_run("completed")

        runs = repository.registry.list_runs()
        row = runs[runs["run_id"] == repository.run_id]
        assert not row.empty
        assert row.iloc[0]["status"] == "completed"

    def test_context_manager(self, temp_base_dir):
        """Should work as context manager."""
        with DataRepository(run_id="ctx12345", base_dir=temp_base_dir, radar="KHTX") as repo:
            assert repo.run_id == "ctx12345"

    def test_generate_run_id(self):
        """Should generate valid run IDs."""
        run_id = DataRepository.generate_run_id("KBOX")
        assert re.match(
            r"^\d{4}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}-\d{4}-KBOX$",
            run_id,
        )


# =========================================================================
# Test: Path Generation
# =========================================================================


class TestPathGeneration:
    """Test path generation methods."""

    def test_generate_plot_path(self, repository):
        """Should generate correct plot path."""
        path = repository.generate_plot_path(
            plot_type="reflectivity",
            scan_time=datetime(2026, 2, 11, 12, 30, 45, tzinfo=UTC),
        )

        assert "KDIX" in str(path)
        assert "plots" in str(path)
        assert "20260211" in str(path)
        assert "reflectivity" in path.name
        assert "123045" in path.name  # HHMMSS
        assert "test1234" in path.name  # run_id
        assert path.suffix == ".png"


# =========================================================================
# Test: Get Latest (PlotConsumer API)
# =========================================================================


class TestGetLatest:
    """Test get_latest method for PlotConsumer polling."""

    def test_get_latest_no_artifacts(self, repository):
        """Should return None when no artifacts exist."""
        result = repository.get_latest(ProductType.ANALYSIS_NC)
        assert result is None

    def test_get_latest_single_artifact(self, repository, temp_base_dir):
        """Should return the only artifact."""
        file_path = temp_base_dir / "KDIX" / "test.nc"
        file_path.touch()

        artifact_id = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file_path,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        result = repository.get_latest(ProductType.ANALYSIS_NC)
        assert result is not None
        assert result["artifact_id"] == artifact_id

    def test_get_latest_returns_most_recent(self, repository, temp_base_dir):
        """Should return artifact with most recent scan_time."""
        file1 = temp_base_dir / "KDIX" / "file1.nc"
        file2 = temp_base_dir / "KDIX" / "file2.nc"
        file3 = temp_base_dir / "KDIX" / "file3.nc"
        file1.touch()
        file2.touch()
        file3.touch()

        repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file1,
            scan_time=datetime(2026, 2, 11, 10, 0, 0, tzinfo=UTC),
            producer="test",
        )
        latest_id = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file2,
            scan_time=datetime(2026, 2, 11, 14, 0, 0, tzinfo=UTC),
            producer="test",
        )
        repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file3,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        result = repository.get_latest(ProductType.ANALYSIS_NC)
        assert result["artifact_id"] == latest_id

    def test_get_latest_filters_by_product_type(self, repository, temp_base_dir):
        """Should only return artifacts of requested type."""
        file1 = temp_base_dir / "KDIX" / "analysis.nc"
        file2 = temp_base_dir / "KDIX" / "gridded.nc"
        file1.touch()
        file2.touch()

        analysis_id = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file1,
            scan_time=datetime(2026, 2, 11, 10, 0, 0, tzinfo=UTC),
            producer="test",
        )
        repository.register_artifact(
            product_type=ProductType.GRIDDED_NC,
            file_path=file2,
            scan_time=datetime(2026, 2, 11, 14, 0, 0, tzinfo=UTC),
            producer="test",
        )

        result = repository.get_latest(ProductType.ANALYSIS_NC)
        assert result["artifact_id"] == analysis_id
        assert result["product_type"] == ProductType.ANALYSIS_NC


class TestGetAllSince:
    """Test get_all_since method for catching up missed artifacts."""

    def test_get_all_since_no_reference(self, repository, temp_base_dir):
        """Should return all artifacts when no reference provided."""
        file1 = temp_base_dir / "KDIX" / "file1.nc"
        file2 = temp_base_dir / "KDIX" / "file2.nc"
        file1.touch()
        file2.touch()

        repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file1,
            scan_time=datetime(2026, 2, 11, 10, 0, 0, tzinfo=UTC),
            producer="test",
        )
        repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file2,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        results = repository.get_all_since(ProductType.ANALYSIS_NC)
        assert len(results) == 2

    def test_get_all_since_with_reference(self, repository, temp_base_dir):
        """Should return only artifacts after reference."""
        file1 = temp_base_dir / "KDIX" / "file1.nc"
        file2 = temp_base_dir / "KDIX" / "file2.nc"
        file3 = temp_base_dir / "KDIX" / "file3.nc"
        file1.touch()
        file2.touch()
        file3.touch()

        id1 = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file1,
            scan_time=datetime(2026, 2, 11, 10, 0, 0, tzinfo=UTC),
            producer="test",
        )
        id2 = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file2,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )
        id3 = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file3,
            scan_time=datetime(2026, 2, 11, 14, 0, 0, tzinfo=UTC),
            producer="test",
        )

        results = repository.get_all_since(ProductType.ANALYSIS_NC, since_artifact_id=id1)
        assert len(results) == 2
        result_ids = [r["artifact_id"] for r in results]
        assert id2 in result_ids
        assert id3 in result_ids
        assert id1 not in result_ids

    def test_get_all_since_returns_chronological_order(self, repository, temp_base_dir):
        """Should return artifacts in chronological order (oldest first)."""
        file1 = temp_base_dir / "KDIX" / "file1.nc"
        file2 = temp_base_dir / "KDIX" / "file2.nc"
        file3 = temp_base_dir / "KDIX" / "file3.nc"
        file1.touch()
        file2.touch()
        file3.touch()

        id1 = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file1,
            scan_time=datetime(2026, 2, 11, 10, 0, 0, tzinfo=UTC),
            producer="test",
        )
        id2 = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file2,
            scan_time=datetime(2026, 2, 11, 14, 0, 0, tzinfo=UTC),
            producer="test",
        )
        id3 = repository.register_artifact(
            product_type=ProductType.ANALYSIS_NC,
            file_path=file3,
            scan_time=datetime(2026, 2, 11, 12, 0, 0, tzinfo=UTC),
            producer="test",
        )

        results = repository.get_all_since(ProductType.ANALYSIS_NC)
        # Should be in chronological order: id1 (10h), id3 (12h), id2 (14h)
        assert results[0]["artifact_id"] == id1
        assert results[1]["artifact_id"] == id3
        assert results[2]["artifact_id"] == id2
