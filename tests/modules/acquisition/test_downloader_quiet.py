# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The nexradaws download call must not leak its own print() chatter to the console.

nexradaws prints "Downloaded <file>" and "<n> out of <m> files downloaded..." straight
to stdout with no quiet option. The acquisition module already logs a controlled
"Downloaded: <name>" line, so the library's duplicate prints are pure clutter and must
be contained at the one call site (no supported quiet flag exists).
"""

from datetime import UTC, datetime

import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader

pytestmark = pytest.mark.unit


def test_download_scan_suppresses_nexradaws_stdout(tmp_path, fake_scan, make_config, capsys):
    class PrintingConn:
        def download(self, files, basepath, keep_aws_folders=False):
            print("Downloaded KOHX_TEST")  # nexradaws chatter
            print("1 out of 1 files downloaded...0 errors")

            class _Results:
                def iter_success(self):
                    return []

            return _Results()

    d = AwsNexradDownloader(make_config(), output_dir=tmp_path, conn=PrintingConn())

    d._download_scan(fake_scan("KOHX_TEST", datetime.now(UTC)), tmp_path / "out.nc")

    out = capsys.readouterr().out
    assert "out of 1 files downloaded" not in out
    assert "Downloaded KOHX_TEST" not in out
