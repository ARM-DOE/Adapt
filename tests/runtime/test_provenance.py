# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Run provenance capture + config hashing for reproducibility."""

from adapt.runtime.provenance import capture_provenance, config_hash


def test_capture_provenance_populates_environment_fields():
    p = capture_provenance()
    assert p.hostname
    assert p.username
    assert "." in p.python_version
    assert p.platform
    assert p.software_version
    # git_commit is a real hex string OR None (faithful — never fabricated)
    assert p.git_commit is None or (
        len(p.git_commit) >= 7 and all(c in "0123456789abcdef" for c in p.git_commit)
    )


def test_config_hash_is_stable_and_sensitive_to_change():
    assert config_hash('{"a": 1}') == config_hash('{"a": 1}')
    assert config_hash('{"a": 1}') != config_hash('{"a": 2}')
    assert len(config_hash("anything")) == 64  # sha256 hex digest
