# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for analysis2d Parquet writes and SQLite schema migration."""

import json
import shutil
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from adapt.persistence import DataRepository

pytestmark = pytest.mark.unit

_T0 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_T1 = datetime(2024, 6, 1, 12, 5, 0, tzinfo=UTC)


@pytest.fixture
def repository():
    d = tempfile.mkdtemp()
    repo = DataRepository(run_id="test1234", base_dir=Path(d), radar="KDIX")
    yield repo
    repo.close()
    shutil.rmtree(d, ignore_errors=True)


def _cells(scan_time, labels, **extra_cols):
    df = pd.DataFrame({"cell_label": labels, "time": [scan_time] * len(labels)})
    for name, values in extra_cols.items():
        df[name] = values
    return df


def _parquet_path(repository):
    return repository.catalog.radar_dir / "analysis" / "analysis2d_test1234.parquet"


class TestWriteAnalysis2dParquet:
    def test_first_write_creates_file_and_registers_item(self, repository):
        item_id = repository.write_analysis2d_parquet(
            _cells(_T0, [1, 2]), scan_time=_T0, parent_ids=["grid-1"]
        )

        stored = pd.read_parquet(_parquet_path(repository))
        assert len(stored) == 2
        # Required columns are stamped in
        assert set(stored["run_id"]) == {"test1234"}
        assert set(stored["radar"]) == {"KDIX"}
        assert "scan_time" in stored.columns

        item = repository.catalog.get_item(item_id)
        assert item["item_type"] == "analysis2d"
        assert json.loads(item["parent_ids"]) == ["grid-1"]
        assert json.loads(item["metadata"])["row_count"] == 2

    def test_second_write_appends_rows(self, repository):
        repository.write_analysis2d_parquet(_cells(_T0, [1]), scan_time=_T0)
        repository.write_analysis2d_parquet(_cells(_T1, [1, 2]), scan_time=_T1)

        stored = pd.read_parquet(_parquet_path(repository))

        assert len(stored) == 3
        assert pd.api.types.is_datetime64_any_dtype(stored["time"])

    def test_schema_evolution_fills_new_column_with_nan(self, repository):
        repository.write_analysis2d_parquet(_cells(_T0, [1]), scan_time=_T0)
        repository.write_analysis2d_parquet(_cells(_T1, [1], hail_score=[0.9]), scan_time=_T1)

        stored = pd.read_parquet(_parquet_path(repository))

        assert stored["hail_score"].isna().sum() == 1
        assert stored["hail_score"].notna().sum() == 1


class TestSqliteSchemaMigration:
    def test_appending_new_columns_alters_table(self, repository):
        db_id = repository.get_or_create_cells_db(scan_time=_T0, producer="processor")
        repository.write_sqlite_table(df=_cells(_T0, [1]), table_name="cells", artifact_id=db_id)

        repository.write_sqlite_table(
            df=_cells(_T1, [1], radar_zdr_max=[2.5], adjacent_cell_uids_json=['["a"]']),
            table_name="cells",
            artifact_id=db_id,
        )

        artifact = repository.get_artifact(db_id)
        with sqlite3.connect(artifact["file_path"]) as conn:
            info = {row[1]: row[2] for row in conn.execute("PRAGMA table_info('cells')")}
            stored = pd.read_sql("SELECT * FROM cells", conn)

        assert info["radar_zdr_max"] == "REAL"  # radar_* convention
        assert info["adjacent_cell_uids_json"] == "TEXT"  # *_json convention
        assert len(stored) == 2


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        ("centroid_x", "INTEGER"),
        ("cell_label", "INTEGER NOT NULL"),
        ("cell_centroid_mass_lat", "REAL"),
        ("centroid_lon", "REAL"),
        ("radar_reflectivity_mean", "REAL"),
        ("speed_max", "REAL"),
        ("cell_area_sqkm", "REAL"),
        ("heading_deg", "REAL"),
        ("time_volume_start", "TIMESTAMP"),
        ("adjacent_cell_uids_json", "TEXT"),
        ("cell_uid", "TEXT"),
    ],
)
def test_infer_sql_type_follows_naming_conventions(column, expected):
    assert DataRepository._infer_sql_type(column) == expected
