# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""read_projection_minute_masks: forward-projection geometry facade.

Yields one record per future minute forecast by the run: current-scan labels
advected forward (resolved by the scan's own cell_uid LUT). When consecutive
scans forecast the same minute, the most recent source scan wins.
"""

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from adapt.persistence.scan_mask_reader import read_projection_minute_masks
from tests.helpers.analysis_nc import cell_block, make_analysis_ds

pytestmark = pytest.mark.unit


@pytest.fixture
def store_coll(tmp_path):
    from adapt.persistence.store import Store, init_store

    root = init_store(tmp_path / "store")
    store = Store.open(root)
    yield store.collection("TEST_RADAR")
    store.close()


def _write(coll, ds, scan_time: str):
    from adapt.persistence.objects import ArtifactMeta, ObjectStore

    objects = ObjectStore(coll.objects_dir, coll.catalog)
    handle = objects.begin(suffix=".nc")
    ds.to_netcdf(handle.staging_path)
    objects.commit(
        handle,
        ArtifactMeta(
            artifact_type="segmentation2d",
            producer="test",
            run_id="MASKRUN1",
            scan_id=f"sid-{scan_time[-8:]}",
            observation_time=datetime.fromisoformat(scan_time).replace(tzinfo=UTC),
        ),
    )


def _two_pair_repo(store_coll):
    """Scans 19:00, 19:03, 19:06; forward projections from 19:03 and 19:06 overlap at 19:07."""
    _write(
        store_coll,
        make_analysis_ds(
            "2024-05-18T19:03:00",
            "2024-05-18T19:00:00",
            cell_labels=cell_block(col=6),
            cell_uids=["uid-A"],
            projection_labels={
                "2024-05-18T19:04:00": cell_block(col=7),
                "2024-05-18T19:05:00": cell_block(col=8),
                "2024-05-18T19:06:00": cell_block(col=9),
                "2024-05-18T19:07:00": cell_block(col=10),
            },
        ),
        "2024-05-18T19:03:00",
    )
    _write(
        store_coll,
        make_analysis_ds(
            "2024-05-18T19:06:00",
            "2024-05-18T19:03:00",
            cell_labels=cell_block(col=9),
            cell_uids=["uid-A"],
            projection_labels={
                "2024-05-18T19:07:00": cell_block(col=11),
                "2024-05-18T19:08:00": cell_block(col=12),
                "2024-05-18T19:09:00": cell_block(col=13),
            },
        ),
        "2024-05-18T19:06:00",
    )


def test_forward_minutes_carry_current_scan_lut(store_coll):
    _two_pair_repo(store_coll)

    rows = read_projection_minute_masks(store_coll, "MASKRUN1")

    records = {pd.Timestamp(r["minute_time"]): r for r in rows}

    r = records[pd.Timestamp("2024-05-18T19:04:00")]
    assert list(r["cell_uid_lut"].astype(str)) == ["NONE", "uid-A"]
    assert r["projection_fraction"] == pytest.approx(1 / 3)
    assert pd.Timestamp(r["source_scan_time"]) == pd.Timestamp("2024-05-18T19:03:00")
    np.testing.assert_array_equal(r["cell_labels"], cell_block(col=7))


def test_most_recent_scan_wins_on_overlap(store_coll):
    """19:07 is forecast by both scans; the 19:06 source (shorter lead) wins."""
    _two_pair_repo(store_coll)

    rows = read_projection_minute_masks(store_coll, "MASKRUN1")

    records = {pd.Timestamp(r["minute_time"]): r for r in rows}

    r = records[pd.Timestamp("2024-05-18T19:07:00")]
    assert pd.Timestamp(r["source_scan_time"]) == pd.Timestamp("2024-05-18T19:06:00")
    assert r["projection_fraction"] == pytest.approx(1 / 3)
    np.testing.assert_array_equal(r["cell_labels"], cell_block(col=11))


def test_records_span_all_forecast_minutes_sorted(store_coll):
    _two_pair_repo(store_coll)

    rows = read_projection_minute_masks(store_coll, "MASKRUN1")
    minutes = [pd.Timestamp(r["minute_time"]) for r in rows]

    assert minutes == sorted(minutes)
    assert minutes == list(pd.date_range("2024-05-18T19:04:00", "2024-05-18T19:09:00", freq="1min"))


def test_missing_cell_uid_raises(store_coll):
    ds = make_analysis_ds(
        "2024-05-18T19:03:00",
        "2024-05-18T19:00:00",
        cell_labels=cell_block(col=6),
        cell_uids=["uid-A"],
        projection_labels={"2024-05-18T19:04:00": cell_block(col=7)},
    ).drop_vars("cell_uid")
    _write(store_coll, ds, "2024-05-18T19:03:00")

    with pytest.raises(ValueError, match="cell_uid"):
        read_projection_minute_masks(store_coll, "MASKRUN1")
