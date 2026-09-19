# tests/test_downloader_init.py
import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader

pytestmark = pytest.mark.unit


def test_init_custom_config(make_config, fake_gateway):
    """Downloader initializes with custom config."""
    from adapt.configuration.schemas.user import UserDownloaderConfig

    config = make_config(
        downloader=UserDownloaderConfig(radar="KDIX", latest_files=5, latest_minutes=60)
    )
    d = AwsNexradDownloader(config, acquire=fake_gateway)

    assert d.config.downloader.radar == "KDIX"
    assert d.config.downloader.latest_files == 5
    assert d.config.downloader.latest_minutes == 60


def test_init_requires_acquisition_gateway(radar_config):
    """Constructing without the acquisition gateway fails loudly."""
    with pytest.raises(ValueError, match="gateway"):
        AwsNexradDownloader(radar_config)


def test_stop_sets_event(radar_config, fake_gateway):
    """Stop event prevents downloader from polling."""
    d = AwsNexradDownloader(radar_config, acquire=fake_gateway)
    assert not d.stopped()
    d.stop()
    assert d.stopped()


def test_historical_mode_from_config(make_config, fake_gateway):
    """Downloader detects historical mode from config.downloader.mode."""
    from adapt.configuration.schemas.user import UserDownloaderConfig

    config = make_config(
        downloader=UserDownloaderConfig(
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-01T01:00:00Z",
        )
    )
    d = AwsNexradDownloader(config, acquire=fake_gateway)
    # Mode is decided by schema, not by is_historical_mode() method
    assert d.config.downloader.mode == "historical"

    d2 = AwsNexradDownloader(make_config(), acquire=fake_gateway)
    assert d2.config.downloader.mode == "realtime"


def test_parse_time_range(make_config, fake_gateway):
    """Downloader parses time range correctly."""
    from adapt.configuration.schemas.user import UserDownloaderConfig

    config = make_config(
        downloader=UserDownloaderConfig(
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-01T01:00:00Z",
        )
    )
    d = AwsNexradDownloader(config, acquire=fake_gateway)

    start, end = d._parse_time_range()

    assert start.tzinfo is not None
    assert end > start
    assert (end - start).total_seconds() == 3600


def test_download_scan_rejects_small_files(fake_scan, radar_config, fake_gateway):
    """Downloaded files below minimum size are discarded (None returned)."""

    class TinyFileConn:
        def download(self, scans, target_dir, keep_aws_folders=False):
            class Result:
                def __init__(self, path):
                    self.filepath = path

            results = []
            for scan in scans:
                path = target_dir / scan.key
                path.write_bytes(b"x")  # below min_file_size
                results.append(Result(path))

            class DownloadResults:
                def iter_success(self):
                    return results

            return DownloadResults()

    d = AwsNexradDownloader(radar_config, acquire=fake_gateway, conn=TinyFileConn())

    assert d._download_scan(fake_scan("tiny")) is None
