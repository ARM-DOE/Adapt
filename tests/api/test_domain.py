# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for domain objects (Run, Track, Scan, ScanBundle).

Synthetic inputs only. No I/O, no database.
"""

import dataclasses
from datetime import UTC, datetime

import pandas as pd
import pytest

from adapt.api.domain import Run, Scan, ScanBundle, Track

pytestmark = pytest.mark.unit

_T0 = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
_T1 = datetime(2024, 6, 1, 14, 0, tzinfo=UTC)


class TestRun:
    def test_run_stores_required_fields(self):
        run = Run(
            run_id="2024JUN01-1200-KDIX",
            radar_id="KDIX",
            start_time=_T0,
            end_time=_T1,
            status="complete",
            mode="historical",
        )
        assert run.run_id == "2024JUN01-1200-KDIX"
        assert run.radar_id == "KDIX"
        assert run.status == "complete"
        assert run.mode == "historical"
        assert run.end_time == _T1

    def test_run_end_time_may_be_none(self):
        run = Run(
            run_id="live-run",
            radar_id="KDIX",
            start_time=_T0,
            end_time=None,
            status="running",
            mode="realtime",
        )
        assert run.end_time is None

    def test_run_is_immutable(self):
        run = Run(
            run_id="r1",
            radar_id="KDIX",
            start_time=_T0,
            end_time=None,
            status="complete",
            mode="historical",
        )
        with pytest.raises((AttributeError, TypeError, dataclasses.FrozenInstanceError)):
            run.run_id = "changed"  # type: ignore[misc]

    def test_equal_runs_are_equal(self):
        kwargs = dict(
            run_id="r1",
            radar_id="KDIX",
            start_time=_T0,
            end_time=None,
            status="complete",
            mode="historical",
        )
        assert Run(**kwargs) == Run(**kwargs)


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

    def test_equal_tracks_are_equal(self):
        kwargs = dict(
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
        assert Track(**kwargs) == Track(**kwargs)


class TestScan:
    def _scan(self):
        return Scan(
            scan_id="sid-aaaa",
            run_id="r1",
            radar_id="KDIX",
            scan_time=_T0,
            source_file_name="KDIX_20240601_120000_V06",
            status="complete",
        )

    def test_scan_stores_required_fields(self):
        scan = self._scan()
        assert scan.scan_id == "sid-aaaa"
        assert scan.scan_time == _T0
        assert scan.source_file_name == "KDIX_20240601_120000_V06"
        assert scan.status == "complete"
        assert scan.start_time is None and scan.end_time is None

    def test_scan_is_immutable(self):
        scan = self._scan()
        with pytest.raises(AttributeError):
            scan.scan_id = "other"


class TestScanBundle:
    def test_bundle_holds_scan_and_optionals(self):
        scan = Scan(
            scan_id="sid-aaaa",
            run_id="r1",
            radar_id="KDIX",
            scan_time=_T0,
            source_file_name="KDIX_20240601_120000_V06",
            status="complete",
        )
        bundle = ScanBundle(scan=scan, segmentation=None, cells=None)
        assert bundle.scan is scan
        assert bundle.segmentation is None
        assert bundle.cells is None
        assert bundle.tracks == []

    def test_bundle_cells_accepts_dataframe(self):
        scan = Scan(
            scan_id="sid-aaaa",
            run_id="r1",
            radar_id="KDIX",
            scan_time=_T0,
            source_file_name="KDIX_20240601_120000_V06",
            status="complete",
        )
        df = pd.DataFrame({"cell_uid": ["a", "b"], "area": [10.0, 20.0]})
        bundle = ScanBundle(scan=scan, segmentation=None, cells=df)
        assert len(bundle.cells) == 2

    def test_bundle_tracks_list_is_mutable(self):
        scan = Scan(
            scan_id="sid-aaaa",
            run_id="r1",
            radar_id="KDIX",
            scan_time=_T0,
            source_file_name="KDIX_20240601_120000_V06",
            status="complete",
        )
        track = Track(
            run_id="r1",
            cell_uid="x",
            first_seen=_T0,
            last_seen=_T1,
            n_scans=1,
            lifetime_s=300.0,
            origin_type="INITIATION",
            termination_type="TERMINATION",
            max_area_km2=5.0,
            max_reflectivity_dbz=40.0,
        )
        bundle = ScanBundle(scan=scan, segmentation=None, cells=None, tracks=[track])
        assert len(bundle.tracks) == 1
        assert bundle.tracks[0].cell_uid == "x"
