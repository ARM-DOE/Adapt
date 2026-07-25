from adapt.configuration.schemas.cli import CLIConfig
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig


def test_cli_overrides_do_not_mutate_user():
    user = UserConfig.model_validate({"RADAR_ID": "KABC", "MODE": "realtime", "BASE_DIR": "/tmp"})

    cli = CLIConfig.model_validate({"radar": "KHTX"})

    internal = resolve_config(ParamConfig(), user, cli)

    # CLI should take precedence
    assert internal.downloader.radar == "KHTX"

    # But the original user model should remain unchanged
    assert user.radar == "KABC"


def test_cli_minimal_overrides_radar_id():
    """CLI radar_id override should work correctly."""
    user = UserConfig(base_dir="/tmp", radar="KABC")
    cli = CLIConfig(radar="KHTX")

    config = resolve_config(ParamConfig(), user, cli)

    assert config.downloader.radar == "KHTX"  # CLI wins
    assert config.base_dir == "/tmp"  # User value preserved


def test_cli_minimal_overrides_mode():
    """CLI mode override should work correctly."""
    user = UserConfig(
        base_dir="/tmp",
        radar="KABC",
        mode="realtime",
        start_time="2024-01-01T00:00:00Z",
        end_time="2024-01-01T12:00:00Z",
    )
    cli = CLIConfig(mode="historical")

    config = resolve_config(ParamConfig(), user, cli)

    assert config.mode == "historical"  # CLI wins
    assert config.downloader.radar == "KABC"  # User value preserved
    # Historical mode validation should pass since start/end times provided


def test_cli_precedence_no_user_config():
    """CLI should work even without UserConfig."""
    cli = CLIConfig(radar="KHTX", mode="realtime")

    # This will need minimal UserConfig for required fields
    user = UserConfig(base_dir="/tmp")
    config = resolve_config(ParamConfig(), user, cli)

    assert config.downloader.radar == "KHTX"
    assert config.mode == "realtime"


def test_cli_only_overrides_specified_fields():
    """CLI should only override fields that are explicitly set."""
    user = UserConfig(base_dir="/tmp", radar="KABC", mode="realtime", min_cellsize_gridpoint=35)

    # CLI only sets radar_id
    cli = CLIConfig(radar="KHTX")

    config = resolve_config(ParamConfig(), user, cli)

    assert config.downloader.radar == "KHTX"  # CLI override
    assert config.mode == "realtime"  # User value preserved
    assert config.segmenter.min_cellsize_gridpoint == 35  # User value preserved
