# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for domain objects (Collection, Run, Track, Scan, ScanBundle, ScanRef).

Synthetic inputs only. No I/O, no database.
"""

import dataclasses
from datetime import UTC, datetime

import pandas as pd
import pytest

from adapt.api.domain import Collection, Run, Scan, ScanBundle, ScanRef, Track

pytestmark = pytest.mark.unit

_T0 = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
_T1 = datetime(2024, 6, 1, 14, 0, tzinfo=UTC)


def _run(**overrides) -> Run:
    kwargs = dict(
        run_id="2024JUN01-1200-KDIX",
        collection_id="KDIX",
        status="completed",
        started_at=_T0,
        ended_at=_T1,
        config_hash="h",
        pipeline_version="1.0",
    )
    kwargs.update(overrides)
    return Run(**kwargs)


class TestCollection:
    def test_collection_stores_fields(self):
        coll = Collection(
            collection_id="KDIX", source_kind="nexrad", location_lat=39.9, location_lon=-74.4
        )
        assert coll.collection_id == "KDIX"
        assert coll.source_kind == "nexrad"


class TestRun:
    def test_run_stores_required_fields(self):
        run = _run()
        assert run.run_id == "2024JUN01-1200-KDIX"
        assert run.collection_id == "KDIX"
        assert run.status == "completed"
        assert run.ended_at == _T1

    def test_run_ended_at_may_be_none(self):
        assert _run(status="running", ended_at=None).ended_at is None

    def test_run_is_immutable(self):
        run = _run()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            run.run_id = "changed"  # type: ignore[misc]

    def test_equal_runs_are_equal(self):
        assert _run() == _run()


class TestTrack:
    def test_track_stores_required_fields(self):
        track = Track(
            run_id="r1",
            cell_uid="abc123",
            first_seen=_T0,
            last_seen=_T1,
            n_scans=24,
            lifetime_s=7200.0,
            origin_type="INITIATION",
            termination_type="TERMINATION",
            max_area_km2=500.0,
            max_reflectivity_dbz=62.3,
        )
        assert track.cell_uid == "abc123"
        assert track.n_scans == 24
        assert track.lifetime_s == 7200.0
        assert track.max_area_km2 == 500.0
        assert track.max_reflectivity_dbz == 62.3

    def test_track_is_immutable(self):
        track = Track(
            run_id="r1",
            cell_uid="abc",
            first_seen=_T0,
            last_seen=_T1,
            n_scans=1,
            lifetime_s=300.0,
            origin_type="INITIATION",
            termination_type="TERMINATION",
            max_area_km2=10.0,
            max_reflectivity_dbz=40.0,
        )
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            track.cell_uid = "changed"  # type: ignore[misc]


def _scan(**overrides) -> Scan:
    kwargs = dict(
        scan_id="abc123def4567890",
        run_id="r1",
        collection_id="KDIX",
        scan_time=_T0,
        source_file_name="KDIX20240601_120000_V06",
        status="complete",
    )
    kwargs.update(overrides)
    return Scan(**kwargs)


class TestScan:
    def test_scan_stores_required_fields(self):
        scan = _scan()
        assert scan.scan_id == "abc123def4567890"
        assert scan.collection_id == "KDIX"
        assert scan.scan_time == _T0
        assert scan.start_time is None  # coverage times are optional metadata

    def test_scan_is_immutable(self):
        scan = _scan()
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            scan.scan_id = "changed"  # type: ignore[misc]


class TestScanRef:
    def test_ref_is_frozen_identity_plus_time(self):
        ref = ScanRef(run_id="r1", scan_id="abc", scan_time=_T0)
        assert ref == ScanRef(run_id="r1", scan_id="abc", scan_time=_T0)
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            ref.scan_id = "changed"  # type: ignore[misc]


class TestScanBundle:
    def test_bundle_holds_scan_and_optionals(self):
        bundle = ScanBundle(scan=_scan(), segmentation=None, cells=None)
        assert bundle.scan.scan_id == "abc123def4567890"
        assert bundle.segmentation is None
        assert bundle.tracks == []

    def test_bundle_cells_accepts_dataframe(self):
        cells = pd.DataFrame({"cell_uid": ["a"], "cell_label": [1]})
        bundle = ScanBundle(scan=_scan(), segmentation=None, cells=cells)
        assert bundle.cells is not None
        assert bundle.cells["cell_uid"].tolist() == ["a"]

    def test_bundle_tracks_list_is_mutable(self):
        bundle = ScanBundle(scan=_scan(), segmentation=None, cells=None)
        bundle.tracks.append("placeholder")  # type: ignore[arg-type]
        assert len(bundle.tracks) == 1
