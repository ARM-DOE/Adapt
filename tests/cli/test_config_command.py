import os
import shutil
import sys
from argparse import Namespace
from pathlib import Path

import pytest

from adapt.cli import _config_cmd


@pytest.fixture
def restore_cwd():
    """Return to the original directory even if the test leaves it deleted."""
    original = Path.cwd()
    yield
    os.chdir(original)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows holds the cwd open; it cannot be deleted, so this state is unreachable",
)
def test_adapt_config_raises_when_cwd_is_missing(tmp_path, restore_cwd):
    """A deleted cwd must fail loudly, not resolve ./config.yaml against nothing."""
    cwd = tmp_path / "gone"
    cwd.mkdir()
    os.chdir(cwd)
    shutil.rmtree(cwd)

    with pytest.raises(FileNotFoundError, match="Current working directory no longer exists"):
        _config_cmd(Namespace(output=None))


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows holds the cwd open; it cannot be deleted, so this state is unreachable",
)
def test_adapt_config_writes_absolute_path_when_cwd_is_missing(tmp_path, restore_cwd):
    """An absolute output path does not need a cwd, so it must still succeed."""
    cwd = tmp_path / "gone"
    cwd.mkdir()
    os.chdir(cwd)
    shutil.rmtree(cwd)

    out = tmp_path / "config.yaml"
    _config_cmd(Namespace(output=str(out)))

    text = out.read_text(encoding="utf-8")
    assert f"base_dir: {tmp_path}" in text
    # Full generated config carries every core section.
    assert "tracker:" in text and "segmenter:" in text


def test_adapt_config_sets_base_dir_to_output_parent(tmp_path):
    out_dir = tmp_path / "nested"
    out_path = out_dir / "my_config.yaml"
    args = Namespace(output=str(out_path))
    _config_cmd(args)

    assert out_path.exists()
    text = out_path.read_text(encoding="utf-8")
    assert f"base_dir: {str(out_dir)}" in text
