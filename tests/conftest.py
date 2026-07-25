"""Root-level pytest fixtures for Adapt test suite.

Provides shared configuration fixtures following Pydantic-based architecture.
All tests must use these fixtures instead of creating raw dict configs.
"""

import shutil
import tempfile
from pathlib import Path

import pytest

from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig
from adapt.execution.nodes.analysis import AnalysisModule
from adapt.execution.nodes.detection import DetectModule
from adapt.execution.nodes.ingest import LoadModule
from adapt.execution.nodes.projection import ProjectionModule
from adapt.execution.nodes.tracking import TrackingModule

# =============================================================================
# Configuration Fixtures (Pydantic-based)
# =============================================================================


@pytest.fixture
def param_config():
    """Expert configuration with all defaults.

    Use this as the base for all test configs. Override specific values
    using user_config or by creating custom UserConfig instances.
    """
    # For tests, provide a default radar_id since it's required at runtime
    from adapt.configuration.schemas.param import ParamConfig as PC

    config = PC()
    # Override radar with a test default (field name is 'radar', not 'radar_id')
    config.downloader.radar = "TEST_RADAR"
    return config


@pytest.fixture
def internal_config(param_config, temp_dir):
    """Fully validated runtime configuration (no overrides).

    Use this when tests don't care about specific config values and just
    need a valid InternalConfig to pass to constructors.

    Examples
    --------
    >>> def test_segmenter_init(internal_config):
    ...     seg = RadarCellSegmenter(internal_config)
    ...     assert seg.method == "conv_strat_raut"  # expert default
    """
    user = UserConfig(base_dir=str(temp_dir))
    return resolve_config(param_config, user, None)


@pytest.fixture
def make_config(param_config, temp_dir):
    """Factory fixture for creating custom test configs.

    Use this when you need to override specific values for a test.
    Returns a callable that accepts UserConfig-compatible kwargs.

    Examples
    --------
    >>> def test_custom_threshold(make_config):
    ...     config = make_config(threshold=35)
    ...     seg = RadarCellSegmenter(config)
    ...     assert seg.threshold == 35.0
    """

    def _make(**user_overrides):
        """Create InternalConfig with user overrides."""
        # Ensure base_dir is always present in tests
        if "base_dir" not in user_overrides:
            user_overrides["base_dir"] = str(temp_dir)

        user = UserConfig(**user_overrides)
        return resolve_config(param_config, user, None)

    return _make


# =============================================================================
# Directory Fixtures
# =============================================================================


@pytest.fixture
def temp_dir():
    """Temporary directory that is cleaned up after test."""
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


# =============================================================================
# Per-Module Config Fixtures
# =============================================================================


@pytest.fixture
def detection_module_config(make_detection_config):
    # Threshold segmenter: the pyart methods (now the default) need a 3D grid
    # NetCDF, which these unit fixtures don't produce. Override method per-test.
    return make_detection_config()


@pytest.fixture
def analysis_module_config(internal_config):
    return AnalysisModule.build_config(internal_config)


@pytest.fixture
def projection_module_config(internal_config):
    return ProjectionModule.build_config(internal_config)


@pytest.fixture
def tracking_module_config(internal_config):
    return TrackingModule.build_config(internal_config)


@pytest.fixture
def ingest_module_config(internal_config):
    return LoadModule.build_config(internal_config)


@pytest.fixture
def make_detection_config(make_config):
    def _make(**kw):
        # Default to the threshold method: unit fixtures build a 2D dataset
        # only, while the pyart methods (the new expert default) classify the
        # 3D grid NetCDF. Tests exercising pyart pass segmentation_method=...
        kw.setdefault("segmentation_method", "threshold")
        # `threshold` is no longer a user-facing field; apply it directly to the
        # built config's method_params so existing call sites keep working.
        threshold = kw.pop("threshold", None)
        cfg = DetectModule.build_config(make_config(**kw))
        if threshold is not None:
            cfg = cfg.model_copy(
                update={"method_params": {**cfg.method_params, "threshold": float(threshold)}}
            )
        return cfg

    return _make


@pytest.fixture
def make_analysis_config(make_config):
    def _make(**kw):
        return AnalysisModule.build_config(make_config(**kw))

    return _make


@pytest.fixture
def make_projection_config(make_config):
    def _make(**kw):
        return ProjectionModule.build_config(make_config(**kw))

    return _make


@pytest.fixture
def make_tracking_config(make_config):
    def _make(**kw):
        return TrackingModule.build_config(make_config(**kw))

    return _make


@pytest.fixture
def make_ingest_config(make_config):
    def _make(**kw):
        return LoadModule.build_config(make_config(**kw))

    return _make


@pytest.fixture
def output_dirs(temp_dir):
    """Standard Adapt output directory structure.

    Returns dict with keys: nexrad, gridnc, analysis, plots, logs
    All directories are created and cleaned up automatically.
    """
    dirs = {
        "nexrad": temp_dir / "nexrad",
        "gridded": temp_dir / "gridded",
        "gridnc": temp_dir / "gridnc",  # Alias for gridded
        "analysis": temp_dir / "analysis",
        "plots": temp_dir / "plots",
        "logs": temp_dir / "logs",
    }

    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    return dirs
