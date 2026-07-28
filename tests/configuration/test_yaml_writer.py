# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for the commented-YAML writer used by config generation.

The writer serializes a plain config dict to YAML, annotating scalar leaves with
their field descriptions as inline comments. Correctness is pinned by round-trip:
parsing the emitted YAML must reproduce the original data exactly.
"""

import pytest

pytestmark = pytest.mark.unit

import yaml  # noqa: E402

from adapt.configuration.schemas.yaml_writer import dump  # noqa: E402


class TestRoundTrip:
    def test_roundtrip_reproduces_data(self):
        data = {
            "mode": "realtime",
            "threshold": 30.0,
            "max_cellsize_gridpoint": None,
            "save_netcdf": True,
            "grid_shape": (41, 301, 301),
            "grid_limits": ((0.0, 20000.0), (-150000.0, 150000.0)),
            "radar_variables": ["reflectivity", "velocity"],
            "global": {"z_level": 2000.0, "var_names": {"reflectivity": "reflectivity"}},
            "module_params": {"cvs": {"gain": 2.5}},
        }
        loaded = yaml.safe_load(dump(data))
        expected = {
            **data,
            "grid_shape": [41, 301, 301],
            "grid_limits": [[0.0, 20000.0], [-150000.0, 150000.0]],
        }
        assert loaded == expected

    def test_roundtrip_preserves_backslashes(self):
        r"""A Windows path must survive the round trip verbatim.

        Every generated config carries base_dir, which on Windows looks like
        C:\Users\...\run. Emitting that as a double-quoted YAML scalar makes the
        backslash an escape character: the value is silently corrupted, and a
        sequence like \U is a hard parse error — so `adapt config` would write a
        config.yaml that `adapt run-nexrad` cannot read.
        """
        data = {
            "base_dir": r"C:\Users\runneradmin\AppData\Local\Temp\nested",
            "note": r"tab\there and a quote ' and a \"double\" quote",
        }
        assert yaml.safe_load(dump(data)) == data


class TestComments:
    def test_scalar_description_becomes_inline_comment(self):
        out = dump({"threshold": 30.0}, {"threshold": "dBZ threshold"})
        assert "threshold: 30.0  # dBZ threshold" in out

    def test_nested_descriptions_apply(self):
        out = dump(
            {"global": {"z_level": 2000.0}},
            {"global": {"z_level": "altitude (m)"}},
        )
        assert "z_level: 2000.0  # altitude (m)" in out

    def test_header_is_prepended(self):
        out = dump({"mode": "realtime"}, header="# Adapt config")
        assert out.startswith("# Adapt config\n")

    def test_no_comment_without_description(self):
        out = dump({"mode": "realtime"})
        assert "mode: realtime" in out
        assert "#" not in out.split("mode: realtime")[1].split("\n")[0]


class TestCommentedSequence:
    """A flat list with a dict description renders block-style and commentable."""

    _DESC = {
        "modules": {
            "_header": "Pipeline modules — all run by default.\nComment out to skip.",
            "ingest": "download + regrid",
            "detection": "",  # no per-item comment
        }
    }

    def test_block_style_with_header_and_item_comments(self):
        out = dump({"modules": ["ingest", "detection"]}, self._DESC)
        lines = out.splitlines()
        assert "# Pipeline modules — all run by default." in lines
        assert "# Comment out to skip." in lines
        assert "modules:" in lines
        assert "  - ingest  # download + regrid" in lines
        assert "  - detection" in lines  # empty desc → no inline comment

    def test_block_list_roundtrips_to_string_list(self):
        out = dump({"modules": ["ingest", "detection"]}, self._DESC)
        assert yaml.safe_load(out)["modules"] == ["ingest", "detection"]

    def test_plain_list_without_dict_desc_stays_inline(self):
        out = dump({"xs": [1, 2, 3]}, {"xs": "numbers"})
        assert "xs: [1, 2, 3]" in out
