# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""format_methods: a clean one-line-per-module summary of the chosen methods.

Built from the frozen InternalConfig at run start — the method (and a few key
parameter choices) for each algorithmic module, never the full parameter set.
"""

import logging

from adapt.runtime.run_reporter import RunReporter, format_methods

_MODULES = ("ingest", "detection", "projection", "analysis", "tracking", "cell_volume_stats")


def test_one_line_per_algorithmic_module(internal_config):
    text = format_methods(internal_config, _MODULES)
    lines = text.splitlines()

    assert lines[0] == "methods:"
    body = lines[1:]
    # ingest, detection, projection, tracking carry a method/approach; the purely
    # parametric modules (analysis, cell_volume_stats) are skipped, not padded.
    assert len(body) == 4
    assert all(len(ln.splitlines()) == 1 for ln in body)


def test_shows_segmentation_and_projection_methods(internal_config):
    text = format_methods(internal_config, _MODULES)

    detection = next(ln for ln in text.splitlines() if ln.strip().startswith("detection"))
    projection = next(ln for ln in text.splitlines() if ln.strip().startswith("projection"))
    assert internal_config.segmenter.method in detection
    assert internal_config.projector.method in projection


def test_shows_key_param_choices_not_full_params(internal_config):
    text = format_methods(internal_config, _MODULES)

    # a few curated choices are welcome...
    assert f"horizon {internal_config.projector.projection_horizon_minutes}min" in text
    # ...but never the full per-method parameter dump
    assert "zr_a" not in text
    assert "conv_wt_threshold" not in text


def test_respects_enabled_modules(internal_config):
    text = format_methods(internal_config, ("detection",))
    body = text.splitlines()[1:]

    assert len(body) == 1
    assert body[0].strip().startswith("detection")
    assert "projection" not in text and "tracking" not in text


def test_no_algorithmic_modules_yields_empty_string(internal_config):
    assert format_methods(internal_config, ("analysis", "cell_volume_stats")) == ""


def test_reporter_methods_emits_console_tagged_record(internal_config):
    captured: list[logging.LogRecord] = []

    class _Cap(logging.Handler):
        def emit(self, record):
            captured.append(record)

    log = logging.getLogger("adapt.test.run_reporter_methods")
    log.handlers[:] = [_Cap()]
    log.setLevel(logging.DEBUG)
    log.propagate = False

    RunReporter(logger=log).methods(internal_config, _MODULES)

    assert captured[0].console is True
    assert captured[0].getMessage().startswith("methods:")
