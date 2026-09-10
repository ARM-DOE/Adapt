# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests that the orchestrator resolves its ingress source by name via the registry."""

import pytest

from adapt.runtime.orchestrator import PipelineOrchestrator

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


class TestOrchestratorSourceResolution:
    def test_default_source_is_aws_nexrad(self, pipeline_config):
        from adapt.modules.acquisition.module import AwsNexradDownloader

        orch = PipelineOrchestrator(pipeline_config)
        orch.collection = orch.store.collection("TEST_RADAR")
        source = orch._create_source()
        assert isinstance(source, AwsNexradDownloader)
        orch.store.close()

    def test_local_directory_source_selected_by_config(self, pipeline_config, tmp_path):
        from adapt.runtime.sources import LocalDirectorySource

        src_dir = tmp_path / "in"
        src_dir.mkdir()
        cfg = pipeline_config.model_copy(
            update={"source": "local_directory", "source_dir": str(src_dir)}
        )
        orch = PipelineOrchestrator(cfg)
        orch.collection = orch.store.collection("TEST_RADAR")
        source = orch._create_source()
        assert isinstance(source, LocalDirectorySource)
        orch.store.close()
