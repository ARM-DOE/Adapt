# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""XlmaStatModule declarations + config building."""

import pytest

from adapt.execution.nodes.xlma_stat import XlmaStatModule
from adapt.modules.base import POSTPROCESS_PHASE
from adapt.modules.xlma_stat.config import XlmaStatConfig

pytestmark = pytest.mark.unit


def test_module_declares_postprocess_phase_and_two_tables():
    m = XlmaStatModule()
    assert m.name == "xlma_stat"
    assert m.pipeline_phase == POSTPROCESS_PHASE
    assert set(m.output_tables) == {"xlma_stat_minutes_rows", "xlma_stat_scan_rows"}
    assert m.output_tables["xlma_stat_minutes_rows"].name == "xlma_stat_minutes"
    assert m.output_tables["xlma_stat_scan_rows"].name == "xlma_stat_scan"


def test_module_consumes_geometry_it_never_computes():
    """The lightning module reads minute masks; it owns no projection inputs."""
    assert XlmaStatModule.inputs == ["xlma_stat_config", "minute_masks", "radar_origin", "run_id"]


def test_output_tables_are_keyed_by_run_and_time():
    m = XlmaStatModule()
    assert m.output_tables["xlma_stat_minutes_rows"].primary_key == (
        "run_id",
        "time",
        "cell_uid",
    )
    assert m.output_tables["xlma_stat_scan_rows"].primary_key == (
        "run_id",
        "scan_time",
        "cell_uid",
    )


def test_config_has_no_clustering_or_projection_knobs():
    assert set(XlmaStatConfig.model_fields) == {"input_dir", "search_radius_m"}


def test_build_config_reads_module_params(make_config):
    cfg = make_config(
        module_params={"xlma_stat": {"input_dir": "/data/lma", "search_radius_m": 750.0}}
    )
    xlma_cfg = XlmaStatModule.build_config(cfg)
    assert isinstance(xlma_cfg, XlmaStatConfig)
    assert xlma_cfg.input_dir == "/data/lma"
    assert xlma_cfg.search_radius_m == 750.0


def test_build_config_without_input_dir_defaults_none(make_config):
    # Config-build must not fail for an unselected module; the node enforces
    # input_dir loudly at run() instead.
    cfg = make_config(module_params={"xlma_stat": {}})
    assert XlmaStatModule.build_config(cfg).input_dir is None
