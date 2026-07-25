# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The package exposes its public API at the top level."""

import pytest

pytestmark = pytest.mark.unit


def test_imports():
    from adapt.consumers.target_selection import (
        CellSnapshot,
        SelectionReason,
        Snapshot,
        TargetSelection,
        TargetSelectionEngine,
        TrajectoryPoint,
        TSEConfig,
        append_selection,
        build_snapshot,
        is_candidate,
        load_config,
        total_score,
    )

    assert callable(load_config)
    assert callable(build_snapshot)
    assert callable(append_selection)
    assert callable(is_candidate)
    assert callable(total_score)
    assert TargetSelectionEngine and TSEConfig and Snapshot
    assert CellSnapshot and TrajectoryPoint and TargetSelection and SelectionReason
