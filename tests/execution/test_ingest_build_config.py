# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""LoadModule.build_config slices reader canonicalization knobs into IngestConfig."""

import pytest

from adapt.execution.nodes.ingest import LoadModule

pytestmark = pytest.mark.unit


def test_ingest_build_config_carries_field_map_and_fields(internal_config):
    ingest_cfg = LoadModule.build_config(internal_config)
    assert ingest_cfg.field_map == {}
    assert ingest_cfg.fields == ()


def test_ingest_build_config_threads_overrides(make_config):
    cfg = make_config(
        reader={
            "field_map": {"corrected_reflectivity": "reflectivity"},
            "fields": ["reflectivity", "velocity"],
        }
    )
    ingest_cfg = LoadModule.build_config(cfg)
    assert ingest_cfg.field_map == {"corrected_reflectivity": "reflectivity"}
    assert ingest_cfg.fields == ("reflectivity", "velocity")
