import queue
import shutil
import tempfile
from pathlib import Path

import pytest

from adapt.configuration.schemas.directories import setup_output_directories
from adapt.configuration.schemas.internal import InternalConfig
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig
from adapt.persistence import DataRepository
from adapt.runtime.file_tracker import FileProcessingTracker


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d)


@pytest.fixture
def tracker(temp_dir):
    db_path = temp_dir / "tracker.db"
    t = FileProcessingTracker(db_path)
    yield t
    t.close()


@pytest.fixture
def pipeline_config(temp_dir) -> InternalConfig:
    """InternalConfig for pipeline tests."""
    param = ParamConfig()
    # For tests, provide defaults since radar_id and base_dir are required at runtime
    user = UserConfig(radar="TEST_RADAR", base_dir=str(temp_dir))
    config_dict = resolve_config(param, user, None).model_dump()

    # Add required fields for new architecture
    output_dirs = setup_output_directories(str(temp_dir))
    config_dict["output_dirs"] = {k: str(v) for k, v in output_dirs.items()}
    config_dict["run_id"] = DataRepository.generate_run_id("TEST")

    return InternalConfig.model_validate(config_dict)


@pytest.fixture
def pipeline_output_dirs(temp_dir):
    """Output directories for pipeline tests.

    Returns dict with 'base' and 'logs' from setup_output_directories,
    plus backward-compatible keys that point to base for legacy tests.
    """
    dirs = setup_output_directories(temp_dir)
    # Add legacy keys for backward compatibility in tests
    dirs["nexrad"] = dirs["base"]
    dirs["gridnc"] = dirs["base"]
    dirs["analysis"] = dirs["base"]
    dirs["plots"] = dirs["base"]
    return dirs


# made for processor tests
@pytest.fixture
def processor_queues():
    return queue.Queue(), queue.Queue()


@pytest.fixture
def test_repository(temp_dir):
    """DataRepository for processor tests."""
    run_id = DataRepository.generate_run_id("TEST")
    repo = DataRepository(run_id=run_id, base_dir=temp_dir, radar="TEST_RADAR")
    yield repo
    repo.close()
    repo.registry.close()
