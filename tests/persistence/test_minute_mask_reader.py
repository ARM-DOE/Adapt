# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""read_minute_masks: the minute-resolution geometry facade for association modules.

Yields one record per whole minute covered by the run: advected
registration_minutes frames (prev-label space, registration LUT) plus, where a
scan time falls exactly on the minute grid, the real segmentation (fraction 0,
the scan's own LUT) replacing the advected frame.
"""

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from adapt.persistence.scan_mask_reader import read_minute_masks
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
    """Scans 19:00, 19:03, 19:06; pair NCs at 19:03 and 19:06."""
    # NC for pair (19:00 -> 19:03): first pair, prev scan untracked -> no reg LUT
    _write(
        store_coll,
        make_analysis_ds(
            "2024-05-18T19:03:00",
            "2024-05-18T19:00:00",
            cell_labels=cell_block(col=6),
            cell_uids=["uid-A"],
            minute_labels={
                "2024-05-18T19:01:00": cell_block(col=4),
                "2024-05-18T19:02:00": cell_block(col=5),
                "2024-05-18T19:03:00": cell_block(col=6),
            },
            registration_uids=None,
        ),
        "2024-05-18T19:03:00",
    )
    # NC for pair (19:03 -> 19:06): prev scan tracked -> reg LUT present
    _write(
        store_coll,
        make_analysis_ds(
            "2024-05-18T19:06:00",
            "2024-05-18T19:03:00",
            cell_labels=cell_block(col=9),
            cell_uids=["uid-A"],
            minute_labels={
                "2024-05-18T19:04:00": cell_block(col=7),
                "2024-05-18T19:05:00": cell_block(col=8),
                "2024-05-18T19:06:00": cell_block(col=9),
            },
            registration_uids=["uid-A"],
        ),
        "2024-05-18T19:06:00",
    )


def test_advected_minutes_carry_registration_lut(store_coll):
    _two_pair_repo(store_coll)

    rows = read_minute_masks(store_coll, "MASKRUN1")

    records = {pd.Timestamp(r["minute_time"]): r for r in rows}

    r = records[pd.Timestamp("2024-05-18T19:04:00")]
    assert list(r["cell_uid_lut"].astype(str)) == ["NONE", "uid-A"]
    assert r["interpolation_fraction"] == pytest.approx(1 / 3)
    assert pd.Timestamp(r["source_scan_time"]) == pd.Timestamp("2024-05-18T19:03:00")
    assert pd.Timestamp(r["target_scan_time"]) == pd.Timestamp("2024-05-18T19:06:00")
    np.testing.assert_array_equal(r["cell_labels"], cell_block(col=7))


def test_scan_minute_uses_real_segmentation(store_coll):
    """At 19:03 the real mask (fraction 0, scan's own LUT) wins over fraction 1.0."""
    _two_pair_repo(store_coll)

    rows = read_minute_masks(store_coll, "MASKRUN1")

    records = {pd.Timestamp(r["minute_time"]): r for r in rows}

    r = records[pd.Timestamp("2024-05-18T19:03:00")]
    assert r["interpolation_fraction"] == 0.0
    np.testing.assert_array_equal(r["cell_labels"], cell_block(col=6))
    assert pd.Timestamp(r["source_scan_time"]) == pd.Timestamp("2024-05-18T19:03:00")


def test_minutes_without_registration_lut_are_skipped(store_coll):
    """The first pair has no prev-scan tracking: its advected minutes are absent."""
    _two_pair_repo(store_coll)

    records = read_minute_masks(store_coll, "MASKRUN1")

    minutes = sorted(pd.Timestamp(r["minute_time"]) for r in records)

    assert pd.Timestamp("2024-05-18T19:01:00") not in minutes
    assert pd.Timestamp("2024-05-18T19:02:00") not in minutes
    # 19:03 is present via the real mask; 19:04..19:06 via the second pair
    assert minutes == list(pd.date_range("2024-05-18T19:03:00", periods=4, freq="1min"))


def test_records_sorted_by_minute(store_coll):
    _two_pair_repo(store_coll)

    minutes = [pd.Timestamp(r["minute_time"]) for r in read_minute_masks(store_coll, "MASKRUN1")]

    assert minutes == sorted(minutes)
