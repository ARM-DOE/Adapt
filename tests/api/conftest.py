# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Shared fixtures for RepositoryClient tests."""

import pytest

from adapt.api.client import RepositoryClient
from tests.api.synthetic_repo import build_synthetic_repo


@pytest.fixture
def repo_root(tmp_path):
    """Minimal synthetic repository with one radar and one run."""
    return build_synthetic_repo(tmp_path)


@pytest.fixture
def client(repo_root):
    c = RepositoryClient(repo_root)
    yield c
    c.close()
