"""NetCDF save retry/raise behaviour for RadarDataLoader._write_netcdf.

The saved NetCDF is a registered downstream artifact, so a persistent write
failure must raise (after retries) rather than be silently swallowed.
"""

import pytest

from adapt.modules.ingest.module import RadarDataLoader

pytestmark = pytest.mark.unit


class _FakeDataset:
    """Minimal stand-in exposing the attributes _write_netcdf touches."""

    def __init__(self, fail_times: int):
        self.data_vars = {"reflectivity": None}
        self._fail_times = fail_times
        self.calls = 0

    def to_netcdf(self, *args, **kwargs):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise OSError("disk full")


def test_write_netcdf_raises_after_retries(tmp_path, make_ingest_config):
    """Persistent write failure raises after exactly netcdf_save_retries attempts."""
    config = make_ingest_config(regridder={"netcdf_save_retries": 3})
    loader = RadarDataLoader(config)
    ds = _FakeDataset(fail_times=99)

    with pytest.raises(OSError, match="disk full"):
        loader._write_netcdf(ds, str(tmp_path), "KLOT_20250305_120000.gz")

    assert ds.calls == 3


def test_write_netcdf_recovers_on_retry(tmp_path, make_ingest_config):
    """A transient failure followed by success does not raise."""
    config = make_ingest_config(regridder={"netcdf_save_retries": 3})
    loader = RadarDataLoader(config)
    ds = _FakeDataset(fail_times=1)

    loader._write_netcdf(ds, str(tmp_path), "KLOT_20250305_120000.gz")

    assert ds.calls == 2
