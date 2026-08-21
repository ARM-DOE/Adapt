from datetime import UTC, datetime, timedelta

import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader


@pytest.mark.integration
def test_real_aws_listing(make_config, fake_gateway):
    """Test real AWS NEXRAD data listing.

    Uses a known radar ID (KMOB) to ensure we get real data from AWS.
    Skips if no scans are available (expected during low-activity periods).
    """
    config = make_config(radar_id="KHTX")  # Use a known radar with consistent data
    d = AwsNexradDownloader(config, acquire=fake_gateway)
    end = datetime.now(UTC)
    start = end - timedelta(minutes=60)
    scans = d._fetch_scans(start, end)

    # Skip if no scans available (integration test depends on real AWS data availability)
    if not scans:
        pytest.skip(
            "No NEXRAD scans available in AWS for the past 60 minutes "
            "(expected during low-activity periods)"
        )
