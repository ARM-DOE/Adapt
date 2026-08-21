# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The conn's download call must not leak any print() chatter to the console.

The acquisition module already logs a controlled "Downloaded: <name>" line, so any
stdout a download backend emits is pure clutter and must be contained at the one call
site. (The retired ``nexradaws`` backend printed such chatter unconditionally; the
guard remains so no backend can leak to the console.)
"""

from datetime import UTC, datetime

import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader

pytestmark = pytest.mark.unit


def test_download_scan_suppresses_conn_stdout(fake_scan, fake_gateway, make_config, capsys):
    class PrintingConn:
        def download(self, files, basepath, keep_aws_folders=False):
            print("Downloaded KOHX_TEST")  # backend chatter
            print("1 out of 1 files downloaded...0 errors")

            class _Results:
                def iter_success(self):
                    return []

            return _Results()

    d = AwsNexradDownloader(make_config(), acquire=fake_gateway, conn=PrintingConn())

    d._download_scan(fake_scan("KOHX_TEST", datetime.now(UTC)))

    out = capsys.readouterr().out
    assert "out of 1 files downloaded" not in out
    assert "Downloaded KOHX_TEST" not in out
