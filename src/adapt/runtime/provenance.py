# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Capture run provenance (git/host/user/python/platform/version) + config hashing.

Lives in the runtime composition layer: the orchestrator captures this once at
run start and hands it to ``ExecutionHistory`` via a ``RunStart`` DTO. Fields
that genuinely do not exist in the environment (e.g. no git checkout) are
recorded as ``None`` — the true value, never a fabricated default.
"""

from __future__ import annotations

import getpass
import hashlib
import platform
import socket
import subprocess

from adapt import __version__
from adapt.contracts.execution_history import RunProvenance

__all__ = ["capture_provenance", "config_hash"]


def _git_commit() -> str | None:
    """Current commit hash, or None when not in a git checkout / git unavailable."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def capture_provenance() -> RunProvenance:
    """Snapshot the execution environment for reproducibility."""
    return RunProvenance(
        git_commit=_git_commit(),
        hostname=socket.gethostname(),
        username=getpass.getuser(),
        python_version=platform.python_version(),
        platform=platform.platform(),
        software_version=__version__,
    )


def config_hash(resolved_config_json: str) -> str:
    """Stable SHA-256 of the resolved configuration JSON (provenance fingerprint)."""
    return hashlib.sha256(resolved_config_json.encode("utf-8")).hexdigest()
