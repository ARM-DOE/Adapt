# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for BaseModule.persistence declaration.

Modules that persist outputs declare a tuple of persistence specs (from
adapt.contracts); modules that persist nothing leave the default empty tuple.
"""

import pytest

pytestmark = pytest.mark.unit

from adapt.modules.base import BaseModule  # noqa: E402


class _NoPersistenceModule(BaseModule):
    name = "no_persistence"

    def run(self, context: dict) -> dict:
        return {}


class TestBaseModulePersistence:
    def test_default_persistence_is_empty(self):
        assert _NoPersistenceModule.persistence == ()

    def test_pure_compute_modules_declare_no_persistence(self):
        from adapt.execution.nodes.projection import ProjectionModule

        assert ProjectionModule.persistence == ()

    def test_decision_logging_modules_declare_only_decision_writes(self):
        """Detection writes no product; its only persistence is the decision log."""
        from adapt.contracts import DecisionTableWrite
        from adapt.execution.nodes.detection import DetectModule

        assert {type(s) for s in DetectModule.persistence} == {DecisionTableWrite}
        assert DetectModule.persistence[0].key == "detection_decisions"

    def test_persisting_modules_declare_specs(self):
        from adapt.contracts import (
            DecisionTableWrite,
            NetcdfArtifact,
            ProductTableWrite,
            TrackTablesWrite,
        )
        from adapt.execution.nodes.analysis import AnalysisModule
        from adapt.execution.nodes.cell_volume_stats import CellVolumeStatsModule
        from adapt.execution.nodes.ingest import LoadModule
        from adapt.execution.nodes.tracking import TrackingModule

        assert {type(s) for s in LoadModule.persistence} == {NetcdfArtifact}
        assert {type(s) for s in AnalysisModule.persistence} == {ProductTableWrite}
        stats, adjacency = AnalysisModule.persistence
        assert stats.table == "cell_stats"
        assert stats.primary_key == ("run_id", "scan_id", "cell_label")
        assert adjacency.table == "cell_adjacency"
        assert adjacency.primary_key == ("run_id", "scan_id", "cell_label_a", "cell_label_b")
        assert {type(s) for s in TrackingModule.persistence} == {
            NetcdfArtifact,
            TrackTablesWrite,
            DecisionTableWrite,
        }
        assert {type(s) for s in CellVolumeStatsModule.persistence} == {ProductTableWrite}
