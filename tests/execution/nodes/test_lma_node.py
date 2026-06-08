# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""LMACellStatisticsModule declarations + config building (no clustering)."""

import pytest

from adapt.execution.nodes.lma import LMACellStatisticsModule
from adapt.modules.base import POSTPROCESS_PHASE
from adapt.modules.lma.config import LMAConfig

pytestmark = pytest.mark.unit


def test_module_declares_postprocess_phase_and_two_tables():
    m = LMACellStatisticsModule()
    assert m.pipeline_phase == POSTPROCESS_PHASE
    assert set(m.output_tables) == {"lma_cell_stats_rows", "lma_flash_attribution_rows"}
    assert m.output_tables["lma_cell_stats_rows"].name == "lma_cell_stats"
    assert m.output_tables["lma_cell_stats_rows"].primary_key == ("cell_uid", "time_bin")
    assert m.output_tables["lma_flash_attribution_rows"].name == "lma_flash_attribution"


def test_build_config_reads_module_params(make_config):
    cfg = make_config(module_params={"lma": {"input_dir": "/data/lma", "search_radius_m": 750.0}})
    lma_cfg = LMACellStatisticsModule.build_config(cfg)
    assert isinstance(lma_cfg, LMAConfig)
    assert lma_cfg.input_dir == "/data/lma"
    assert lma_cfg.search_radius_m == 750.0


def test_build_config_without_input_dir_defaults_none(make_config):
    # Config-build must not fail for an unselected module; the node enforces
    # input_dir loudly at run() instead.
    cfg = make_config(module_params={"lma": {}})
    assert LMACellStatisticsModule.build_config(cfg).input_dir is None
