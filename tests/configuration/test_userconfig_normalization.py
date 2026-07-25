from adapt.configuration.schemas.user import UserConfig


def test_uppercase_keys_are_handled():
    raw = {
        "MODE": "historical",
        "RADAR_ID": "KHTX",
        "MIN_CELLSIZE_GRIDPOINT": 40,
        "BASE_DIR": "/tmp/adapt_out",
    }

    user = UserConfig.model_validate(raw)

    assert user.mode == "historical"
    assert user.radar == "KHTX"
    assert user.min_cellsize_gridpoint == 40
    assert user.base_dir == "/tmp/adapt_out"


def test_unknown_keys_do_not_reach_overrides():
    # Undeclared keys are captured (extra="allow") so real InternalConfig sections
    # can pass through, but a legacy/unknown key that is not a routed section must
    # never reach the resolved overrides.
    raw = {"MODE": "realtime", "UNKNOWN_LEGACY": 12345}
    user = UserConfig.model_validate(raw)

    assert user.mode == "realtime"
    assert "UNKNOWN_LEGACY" not in user.to_internal_overrides()
