# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Reading per-scan cell masks (labels + grid + cell_uid LUT) from analysis NetCDFs.

This is the repository read the PostProcessor injects into post-process modules,
which cannot import persistence themselves.
"""

import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from adapt.persistence import DataRepository, ProductType
from adapt.persistence.scan_mask_reader import read_scan_masks

pytestmark = pytest.mark.unit


@pytest.fixture
def repo():
    d = Path(tempfile.mkdtemp())
    r = DataRepository(run_id="MASK1", base_dir=d, radar="TEST_RADAR")
    yield r
    r.close()
    r.registry.close()
    shutil.rmtree(d, ignore_errors=True)


def _analysis_ds():
    labels = np.zeros((3, 3), dtype=np.int32)
    labels[1, 1] = 1
    return xr.Dataset(
        {
            "cell_labels": (("y", "x"), labels),
            "cell_uid": ("cell_label", np.array(["NONE", "uid-A"], dtype=np.str_)),
        },
        coords={
            "x": np.array([0.0, 200.0, 400.0]),
            "y": np.array([0.0, 200.0, 400.0]),
            "cell_label": np.arange(2),
        },
    )


def test_reads_one_record_per_analysis_scan(repo):
    for hh in (12, 13):
        repo.write_netcdf(
            ds=_analysis_ds(),
            product_type=ProductType.ANALYSIS_NC,
            scan_time=datetime(2024, 5, 18, hh, 0, 0, tzinfo=UTC),
            producer="test",
        )

    masks = read_scan_masks(repo)

    assert len(masks) == 2
    m = masks[0]
    assert m["cell_labels"].shape == (3, 3)
    assert list(m["x"]) == [0.0, 200.0, 400.0]
    assert list(m["cell_uid_lut"].astype(str)) == ["NONE", "uid-A"]


def test_raises_when_cell_uid_lut_missing(repo):
    ds = _analysis_ds().drop_vars("cell_uid")
    repo.write_netcdf(
        ds=ds,
        product_type=ProductType.ANALYSIS_NC,
        scan_time=datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC),
        producer="test",
    )
    with pytest.raises(ValueError, match="cell_uid"):
        read_scan_masks(repo)
