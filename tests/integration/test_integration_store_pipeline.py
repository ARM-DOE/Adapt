# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Vertical store gate on real NEXRAD volumes.

adapt init → REAL pipeline run (source → acquisition gateway → ingest →
detection → tracking → enrichment → store) on three consecutive Level-II
volumes, then proves the store contract end to end: the exact on-disk layout,
every object cataloged with checksum + lineage back to its raw volume,
complete-scan semantics, and every dashboard workflow (timeline, rasters,
hover, tracks, movie with per-frame closes, target selection, volume stats,
lightning postprocess) through the public StoreClient only.

Volumes come from ``ADAPT_REALDATA_DIR`` (a directory of ``*_V06`` files) when
set, else three consecutive volumes are downloaded from the NEXRAD archive.
No binary fixtures are stored in the repository.

Excluded from default CI (integration marker); run locally:

    ADAPT_REALDATA_DIR=$TMPDIR/nexrad_real pytest tests/integration -m integration
"""

import hashlib
import os
import queue
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from adapt.api.store_client import StoreClient
from adapt.configuration.schemas.internal import InternalConfig
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig
from adapt.consumers.live._utils import cells_for_scan
from adapt.persistence.errors import StoreError
from adapt.persistence.execution_history import StoreExecutionHistory
from adapt.persistence.store import Store, init_store
from adapt.persistence.store_registry import RunStart, StoreRegistry
from adapt.runtime.acquire import StoreAcquirer
from adapt.runtime.processor import RadarProcessor
from adapt.runtime.sources import LocalDirectorySource

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


def _visible(path: Path) -> set[str]:
    """Directory entries minus SQLite WAL/SHM sidecars and staging scratch."""
    return {
        p.name
        for p in path.iterdir()
        if not p.name.endswith(("-wal", "-shm")) and p.name != ".staging"
    }


def _open_fds() -> int:
    return len(os.listdir("/dev/fd"))


def test_integration_store_pipeline_end_to_end(tmp_path):
    volumes = _real_volumes(tmp_path)
    radar = volumes[0].name[:4]

    # ── adapt init: exact layout, once ────────────────────────────────────────
    root = init_store(tmp_path / "store")
    assert _visible(root) == {"registry.db", "logs", "collections"}
    with pytest.raises(StoreError, match="already initialized"):
        init_store(root)

    param = ParamConfig()
    user = UserConfig(radar=radar, base_dir=str(root))
    config_dict = resolve_config(param, user, None).model_dump()
    run_id = f"2026AUG13-1731-{radar}"
    config_dict["run_id"] = run_id
    config = InternalConfig.model_validate(config_dict)

    # ── real run through the store composition ───────────────────────────────
    store = Store.open(root)
    collection = store.collection(radar)
    registry = StoreRegistry.get_instance(root)
    registry.register_collection(radar, source_kind="nexrad")
    registry.begin_run(
        RunStart(
            run_id=run_id,
            collection_id=radar,
            config_hash="gate",
            config_json=config.model_dump_json(),
            pipeline_version="gate",
            environment_json="{}",
        )
    )
    history = StoreExecutionHistory(registry)
    acquirer = StoreAcquirer(collection, run_id)

    source = LocalDirectorySource(
        SimpleNamespace(
            source_dir=str(volumes[0].parent),
            downloader=SimpleNamespace(min_file_size=1),
        ),
        result_queue=queue.Queue(),
        acquire=acquirer,
    )
    source.run()

    proc = RadarProcessor(
        queue.Queue(),
        config,
        collection=collection,
        registry=registry,
        run_id=run_id,
        history=history,
    )
    processed = 0
    while processed < _N_VOLUMES and not source._result_queue.empty():
        assert proc.process_file(source._result_queue.get_nowait()) is True
        processed += 1
    assert processed == _N_VOLUMES
    registry.finalize_run(run_id, "completed", scans_processed=processed)
    store.close()
    registry.close()

    # ── exact layout after the run: nothing else ever appears ────────────────
    assert _visible(root) == {"registry.db", "logs", "collections"}
    assert _visible(root / "collections") == {radar}
    assert _visible(root / "collections" / radar) == {"catalog.db", "products.db", "objects"}

    client = StoreClient(root)
    try:
        # ── every object cataloged: checksum + lineage to the raw volume ─────
        artifacts = client.artifacts(radar)
        rows_by_object = {row["object_name"]: row for row in artifacts}
        object_files = sorted(
            p for p in (root / "collections" / radar / "objects").iterdir() if p.name != ".staging"
        )
        assert len(object_files) >= _N_VOLUMES * 3  # raw + gridded3d + segmentation2d(2..N)
        for object_file in object_files:
            row = rows_by_object[object_file.name]  # KeyError = uncataloged object
            assert row["checksum_sha256"] == hashlib.sha256(object_file.read_bytes()).hexdigest()
            assert row["size_bytes"] == object_file.stat().st_size
        orphans = client.sql(
            "SELECT a.artifact_id, a.artifact_type FROM catalog.artifacts a"
            " WHERE a.artifact_type != 'raw_volume' AND NOT EXISTS ("
            "   SELECT 1 FROM catalog.artifact_lineage l"
            "   JOIN catalog.artifacts p ON p.artifact_id = l.parent_artifact_id"
            "   WHERE l.child_artifact_id = a.artifact_id"
            "     AND p.artifact_type = 'raw_volume')",
            radar,
        )
        assert orphans.empty, f"artifacts without raw lineage:\n{orphans}"

        # ── scans: content identity; pairs complete, the first stays pending ──
        expected_ids = [hashlib.sha256(v.read_bytes()).hexdigest()[:16] for v in volumes]
        complete = client.scans(radar, run_id=run_id)
        assert [s.scan_id for s in complete] == expected_ids[1:]
        statuses = client.sql("SELECT scan_id, status FROM catalog.scans", radar)
        by_scan = dict(zip(statuses["scan_id"], statuses["status"], strict=True))
        assert by_scan[expected_ids[0]] == "pending"  # first scan has no pair → no analysis

        # ── dashboard timeline + rasters (in-memory, fd-stable) ──────────────
        timeline = client.scan_timeline(radar, run_id)
        assert [ref.scan_id for ref in timeline] == expected_ids[1:]
        baseline_fds = _open_fds()
        for _ in range(15):
            for ref in timeline:
                with client.open_scan_raster(radar, ref.run_id, ref.scan_id) as raster:
                    assert raster.dataset.attrs["scan_id"] == ref.scan_id
                    assert float(raster.dataset["reflectivity"].max()) > 0
        assert _open_fds() <= baseline_fds + 2  # frequent reads retain no handles

        # ── live-follow watermark ─────────────────────────────────────────────
        assert client.scans_since(radar, run_id, after=timeline[0]) == timeline[1:]
        assert client.scans_since(radar, run_id, after=timeline[-1]) == []

        # ── cells, hover helper, tracks, graph ────────────────────────────────
        cells = client.cells(run_id, radar)
        assert not cells.empty
        for ref in timeline:
            scan_cells = client.cells_at_scan(run_id, ref.scan_id, radar)
            assert not scan_cells.empty
            hover = cells_for_scan(cells, ref.scan_id, int(scan_cells.iloc[0]["cell_label"]))
            assert len(hover) == 1
        uid = str(cells.iloc[0]["cell_uid"])
        history_df = client.track_history(run_id, uid, radar)
        assert not history_df.empty
        assert history_df["scan_time"].is_monotonic_increasing
        graph = client.track_graph(run_id, uid, radar)
        assert uid in graph.cell_uids

        # ── generic reads: operator filters + read-only SQL ───────────────────
        stats = client.table(
            "cell_stats",
            radar,
            run_id=run_id,
            filters={"cell_area_sqkm": {"op": "ge", "value": 0.0}},
        )
        assert not stats.empty
        counted = client.sql("SELECT COUNT(DISTINCT scan_id) AS n FROM cell_stats", radar).iloc[0][
            "n"
        ]
        assert counted == len(timeline)

        # ── volume-stats enrichment landed as a products table ────────────────
        known_tables = set(client.tables(radar)["table_name"])
        assert "cell_volume_stats" in known_tables
        vol = client.table("cell_volume_stats", radar, run_id=run_id)
        assert not vol.empty
        assert {"run_id", "scan_id", "cell_uid"} <= set(vol.columns)

        # ── movie export: one frame per scan, raster closed per frame ─────────
        from adapt.consumers.live._movie import MovieSpec, write_movie_frames
        from adapt.consumers.live._renderer import OverlayData, ViewState, scan_frame_drawer

        closes: list[str] = []

        def open_raster(i):
            raster = client.open_scan_raster(radar, timeline[i].run_id, timeline[i].scan_id)
            original = raster.close

            def counting_close():
                closes.append(timeline[i].scan_id)
                original()

            raster.close = counting_close  # type: ignore[method-assign]
            return raster

        view = ViewState(
            var_name="reflectivity",
            vmin=10.0,
            vmax=60.0,
            bg_alpha=0.35,
            max_proj_steps=0,
            show_flow=False,
            zoom=None,
            selected_cells={},
            color_slots=("#e15759",),
        )
        draw = scan_frame_drawer(open_raster, view, OverlayData(cell_df=None, track_histories={}))
        movie = tmp_path / "gate.gif"
        frames = list(
            write_movie_frames(
                MovieSpec(n_frames=2, draw_frame=draw, figsize=(4.0, 3.0), dpi=60), movie, fps=2
            )
        )
        assert frames == [0, 1]
        assert movie.stat().st_size > 0
        assert closes == [timeline[0].scan_id, timeline[1].scan_id]

        # ── target selection replay frame on the real raster ──────────────────
        from matplotlib.figure import Figure

        from adapt.consumers.live._targeting import draw_tse_map
        from adapt.consumers.target_selection.repository_source import build_snapshot

        snap = build_snapshot(client, run_id, radar, growth_window_scans=4)
        assert snap.cells
        fig = Figure(figsize=(4, 4))
        ax = fig.add_subplot(111)
        with client.open_scan_raster(radar, run_id, timeline[-1].scan_id) as raster:
            draw_tse_map(
                ax,
                snap.scan_time,
                raster.dataset,
                snap,
                None,
                [c.uid for c in snap.cells],
                raise_errors=True,
            )

        # ── lightning postprocess over the finished run (after-the-fact) ──────
        pytest.importorskip("pyproj")
        from adapt.runtime.postprocessor import PostProcessor
        from tests.helpers.lma import write_flash_sorted_nc

        registry_2 = StoreRegistry.get_instance(root)
        store_2 = Store.open(root)
        collection_2 = store_2.collection(radar)
        origin = registry_2.get_collection(radar)
        assert origin["location_lat"] is not None  # recorded from the first grid
        lma_dir = tmp_path / "lma"
        lma_dir.mkdir()
        minute = timeline[-1].scan_time.strftime("%y%m%d_%H%M")
        write_flash_sorted_nc(
            lma_dir / f"LYLOUT_{minute}00_0600_map.nc",
            timeline[-1].scan_time.strftime("%Y-%m-%dT%H:%M:10"),
            2,
            lat=origin["location_lat"],
            lon=origin["location_lon"],
        )
        lma_config = config.model_copy(
            update={"module_params": {"xlma_stat": {"input_dir": str(lma_dir)}}}
        )
        PostProcessor(collection_2, registry_2, run_id, lma_config).run(modules=["xlma_stat"])
        assert "xlma_stat_minutes" in set(client.tables(radar)["table_name"])
        store_2.close()
        registry_2.close()

        # ── store health + registry lifecycle read-back ───────────────────────
        Store.open(root).validate_collection(radar)
        progress = client.pipeline_progress(radar)
        assert progress["status"] == "completed"
        assert progress["complete_scans"] == len(timeline)
        assert client.is_pipeline_running(radar) is False
    finally:
        client.close()

    # ── legacy roots fail loudly ───────────────────────────────────────────────
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    (legacy / "adapt_registry.db").touch()
    with pytest.raises(StoreError, match="obsolete"):
        StoreClient(legacy)
