# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Vertical scan-identity gate on real NEXRAD volumes.

Runs the REAL pipeline (source → ingest → detection → tracking → persistence)
on three consecutive Level-II volumes, then proves one identity spans every
layer a consumer touches: the scans registry, tracking rows, catalog items,
artifact attrs, the public API, and the dashboard's row-lookup helper.

Volumes come from ``ADAPT_REALDATA_DIR`` (a directory of ``*_V06`` files) when
set, else three consecutive volumes are downloaded from the NEXRAD archive.
No binary fixtures are stored in the repository.

Excluded from default CI (integration marker); run locally:

    ADAPT_REALDATA_DIR=/path/to/volumes pytest tests/integration/test_integration_scan_identity.py
"""

import hashlib
import os
import queue
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import xarray as xr

from adapt.api.client import RepositoryClient
from adapt.configuration.schemas.directories import setup_output_directories
from adapt.configuration.schemas.internal import InternalConfig
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig
from adapt.consumers.live._utils import cells_for_scan
from adapt.persistence import DataRepository
from adapt.runtime.processor import RadarProcessor
from adapt.runtime.sources import LocalDirectorySource
from adapt.utils.time import from_scan_iso

pytestmark = pytest.mark.integration

_N_VOLUMES = 3


def _real_volumes(tmp_path) -> list[Path]:
    env = os.environ.get("ADAPT_REALDATA_DIR")
    if env:
        files = sorted(Path(env).glob("*_V06"))[:_N_VOLUMES]
        if len(files) < _N_VOLUMES:
            pytest.skip(f"ADAPT_REALDATA_DIR has fewer than {_N_VOLUMES} *_V06 volumes")
        return files

    from adapt.downloaders import NexradS3

    conn = NexradS3()
    scans = conn.get_avail_scans_in_range(
        datetime(2013, 5, 20, 19, 0, tzinfo=UTC),
        datetime(2013, 5, 20, 20, 0, tzinfo=UTC),
        "KTLX",
    )
    if len(scans) < _N_VOLUMES:
        pytest.skip("NEXRAD archive returned too few volumes")
    dest = tmp_path / "volumes"
    dest.mkdir()
    results = conn.download(scans[:_N_VOLUMES], dest)
    return sorted(Path(r.filepath) for r in results.iter_success())


def test_integration_scan_identity_end_to_end(tmp_path):
    volumes = _real_volumes(tmp_path)
    radar = volumes[0].name[:4]
    root = tmp_path / "store"
    root.mkdir()

    param = ParamConfig()
    user = UserConfig(radar=radar, base_dir=str(root))
    config_dict = resolve_config(param, user, None).model_dump()
    output_dirs = setup_output_directories(str(root))
    config_dict["output_dirs"] = {k: str(v) for k, v in output_dirs.items()}
    run_id = DataRepository.generate_run_id(radar)
    config_dict["run_id"] = run_id
    config = InternalConfig.model_validate(config_dict)

    repo = DataRepository(run_id=run_id, base_dir=root, radar=radar)
    try:
        # The real replay source mints identity at the boundary; the processor
        # consumes its messages verbatim.
        source = LocalDirectorySource(
            SimpleNamespace(
                source_dir=str(volumes[0].parent),
                downloader=SimpleNamespace(min_file_size=1),
            ),
            result_queue=queue.Queue(),
        )
        source.run()

        proc = RadarProcessor(queue.Queue(), config, output_dirs, repository=repo)
        processed = 0
        while processed < _N_VOLUMES and not source._result_queue.empty():
            assert proc.process_file(source._result_queue.get_nowait()) is True
            processed += 1
        assert processed == _N_VOLUMES
    finally:
        repo.close()
        repo.registry.close()

    client = RepositoryClient(root)
    try:
        # 1. The scans registry is real: every processed volume is discoverable
        #    with its content-derived identity.
        scans = client.scans(radar, run_id=run_id)
        assert len(scans) == _N_VOLUMES
        by_name = {v.name: v for v in volumes}
        for scan in scans:
            expected = hashlib.sha256(by_name[scan.source_file_name].read_bytes())
            assert scan.scan_id == expected.hexdigest()[:16]

        # 2. Tracking rows key on the same identity (scans 2..N have pairs).
        tracked = [s for s in scans[1:]]
        uid_seen: dict[str, int] = {}
        for scan in tracked:
            cells = client.cells_at_scan(run_id, scan.scan_id, radar=radar)
            assert not cells.empty, f"no cells for scan {scan.scan_id}"
            assert (cells["scan_id"] == scan.scan_id).all()
            for uid in cells["cell_uid"]:
                uid_seen[uid] = uid_seen.get(uid, 0) + 1

        # 3. A cell tracked across consecutive scans has a time-ordered history.
        persistent = [u for u, n in uid_seen.items() if n >= 2]
        assert persistent, "no cell persisted across consecutive scans"
        history = client.track_history(run_id, persistent[0], radar=radar)
        assert len(history) >= 2
        times = [from_scan_iso(t) for t in history["scan_time"]]
        assert times == sorted(times)

        # 4. Artifacts are self-describing and catalog-consistent: attrs carry
        #    the same identity the catalog rows do.
        artifacts = client.artifacts(product_type="segmentation2d", radar=radar, run_id=run_id)
        assert len(artifacts) == len(tracked)
        for row in artifacts.itertuples():
            ds = xr.open_dataset(root / radar / row.file_path)
            try:
                assert ds.attrs["scan_id"] == row.scan_id
                assert ds.attrs["scan_time"] == row.scan_time
            finally:
                ds.close()

        # 5. The dashboard's hover lookup resolves rows for a rendered scan by
        #    the identity stamped in the artifact — no time comparison anywhere.
        all_cells = client.table("cells_by_scan", radar=radar, run_id=run_id)
        last = tracked[-1]
        label = int(client.cells_at_scan(run_id, last.scan_id, radar=radar)["cell_label"].iloc[0])
        rows = cells_for_scan(all_cells, last.scan_id, label)
        assert len(rows) == 1

        # 6. scan_bundle resolves every product for the scan by identity.
        bundle = client.scan_bundle(run_id, last.scan_id, radar=radar)
        assert bundle.segmentation is not None
        assert bundle.cells is not None and not bundle.cells.empty
        assert bundle.tracks
    finally:
        client.close()
