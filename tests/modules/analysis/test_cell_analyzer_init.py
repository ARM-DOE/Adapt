import pytest

from adapt.modules.analysis.module import RadarCellAnalyzer

pytestmark = pytest.mark.unit


def test_init_with_default_config(make_analysis_config):
    """Analyzer initializes with default config."""
    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)

    assert analyzer.reflectivity_field == "reflectivity"
    assert analyzer.max_projection_steps > 0


def test_init_custom_config(make_analysis_config):
    """Analyzer initializes with a custom tracking field (the role knob).

    Note: the old REFLECTIVITY_VAR alias is a NAME mapping handled at
    ingest (reader.field_map); selecting a different analysis field is
    global.tracking_field, and the analyzer whitelist must include it.
    """
    from adapt.configuration.schemas.user import UserGlobalConfig, UserProjectorConfig

    config = make_analysis_config(
        global_=UserGlobalConfig(tracking_field="dbz"),
        radar_variables=["dbz"],
        projector=UserProjectorConfig(max_projection_steps=2),
    )
    analyzer = RadarCellAnalyzer(config)

    assert analyzer.reflectivity_field == "dbz"
    assert analyzer.max_projection_steps == 2
