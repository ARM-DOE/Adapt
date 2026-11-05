# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Decision-record contracts: bundles are internally consistent and frozen."""

import dataclasses

import pytest

from adapt.contracts import (
    ContractViolation,
    SegmentationDecisions,
    SegmentationFrame,
    TrackingDecisions,
    TrackingScan,
    check_segmentation_decisions,
    check_tracking_decisions,
)

pytestmark = pytest.mark.unit


def _tracking_scan(**kw) -> TrackingScan:
    counts = dict.fromkeys((f.name for f in dataclasses.fields(TrackingScan)), 0)
    counts.update(dt_s=None, reset_code="FIRST_SCAN")
    return TrackingScan(**{**counts, **kw})


def _bundle(scan: TrackingScan) -> TrackingDecisions:
    return TrackingDecisions(scan, (), (), (), (), ())


def test_tracking_bundle_with_no_pairs_is_valid():
    check_tracking_decisions(_bundle(_tracking_scan()))


def test_tracking_pair_rows_must_match_pair_count():
    with pytest.raises(ContractViolation):
        check_tracking_decisions(_bundle(_tracking_scan(n_pairs=1)))


def test_tracking_cell_rows_must_match_current_cell_count():
    with pytest.raises(ContractViolation):
        check_tracking_decisions(_bundle(_tracking_scan(n_curr=2)))


def test_terminations_cannot_exceed_previous_and_latent_tracks():
    with pytest.raises(ContractViolation):
        check_tracking_decisions(_bundle(_tracking_scan(n_prev=1, n_latent=1, n_termination=3)))


def test_segmentation_component_rows_must_match_component_count():
    frame = SegmentationFrame(0, 0, 1, 0, 0, 0, 0, 0, 0, 0)
    with pytest.raises(ContractViolation):
        check_segmentation_decisions(SegmentationDecisions(frame, (), ()))


def test_records_are_frozen():
    frame = _tracking_scan()
    with pytest.raises(dataclasses.FrozenInstanceError):
        frame.n_prev = 3  # type: ignore[misc]
