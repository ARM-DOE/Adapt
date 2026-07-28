from argparse import Namespace

from adapt.configuration.schemas.initialization import init_runtime_config


def _args(tmp_path, radar="KPOE", rerun=True):
    return Namespace(
        config=None,
        radar=radar,
        mode="realtime",
        start_time=None,
        end_time=None,
        base_dir=str(tmp_path),
        verbose=False,
        run_id=None,
        rerun=rerun,
        max_runtime=1,
        no_plot=True,
        plot_interval=2.0,
        show_plots=False,
    )


def test_rerun_cleanup_does_not_delete_user_files(tmp_path, capsys):
    # User-owned file in base dir (must survive)
    cfg = tmp_path / "config.yaml"
    cfg.write_text("user config\n", encoding="utf-8")
    notes = tmp_path / "notes"
    notes.mkdir()
    (notes / "keep.txt").write_text("keep\n", encoding="utf-8")

    radar = "KPOE"
    # Program-created radar output dir (should be deleted)
    radar_dir = tmp_path / radar
    (radar_dir / "analysis").mkdir(parents=True)
    (radar_dir / "analysis" / "KPOE_processing_tracker.db").write_text("db", encoding="utf-8")

    # Program-created runtime config (should be deleted)
    (tmp_path / f"runtime_config_2026APR04-0000-{radar}.json").write_text("{}", encoding="utf-8")

    # Program-created legacy pipeline catalog (should be deleted)
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()
    (catalog_dir / f"2026APR04-0000-{radar}_pipeline_catalog.db").write_text("db", encoding="utf-8")

    # Trigger init with rerun cleanup
    init_runtime_config(_args(tmp_path, radar=radar, rerun=True))
    _ = capsys.readouterr()

    assert cfg.exists()
    assert (notes / "keep.txt").exists()

    assert not radar_dir.exists()
    assert not (tmp_path / f"runtime_config_2026APR04-0000-{radar}.json").exists()
    assert not (catalog_dir / f"2026APR04-0000-{radar}_pipeline_catalog.db").exists()
