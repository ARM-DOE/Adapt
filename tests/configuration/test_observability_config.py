# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Observability toggles live on the existing logging config section."""

from adapt.configuration.schemas.internal import InternalLoggingConfig


def test_logging_config_carries_observability_defaults() -> None:
    cfg = InternalLoggingConfig(level="INFO")
    assert cfg.enabled is True
    assert cfg.traces is True
    assert cfg.metrics is True
    assert cfg.json_logs is False
    assert cfg.console_logs is True
    assert cfg.console_level == "WARNING"
    assert cfg.progress_every == 30.0
