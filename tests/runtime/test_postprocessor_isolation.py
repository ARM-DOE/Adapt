# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Post-process modules are isolated from the live pipeline by registration.

The live pipeline discovers modules only from defaults.yaml (+ pipeline
extensions); post-process modules live in postprocess_defaults.yaml and are
loaded only by the PostProcessor. Because the two discovery sources are disjoint,
the proven RadarProcessor path never imports — and so never sees — a post-process
module, with no edit to RadarProcessor.
"""

from pathlib import Path

import pytest
import yaml

import adapt.runtime.postprocessor as pp
from adapt.execution.pipeline_builder import _DEFAULTS_YAML

pytestmark = pytest.mark.unit


def _module_paths(yaml_path: Path) -> set[str]:
    cfg = yaml.safe_load(yaml_path.read_text()) or {}
    return set((cfg.get("pipeline") or cfg.get("postprocess") or {}).get("modules", []) or [])


def test_pipeline_and_postprocess_discovery_sources_are_disjoint():
    live = _module_paths(Path(_DEFAULTS_YAML))
    post = _module_paths(pp._POSTPROCESS_DEFAULTS_YAML)

    assert "adapt.execution.nodes.lma" in post
    assert "adapt.execution.nodes.lma" not in live
    assert live.isdisjoint(post)
