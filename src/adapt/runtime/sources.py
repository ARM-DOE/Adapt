# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Ingress source plugins and registry.

A source is a swappable, registered-by-name plugin that acquires raw scans into
the store (via the injected acquisition gateway) and feeds the resulting queue
messages to the processor. The orchestrator resolves the source named by
``config.source`` and constructs it uniformly with
(config, result_queue, acquire).

Built-in sources:
- ``aws_nexrad``      — AwsNexradDownloader (download from S3, realtime/historical)
- ``local_directory`` — LocalDirectorySource (acquire already-present files in order)

Adding a source: implement the ScanSource interface (see contracts/source.py) and
call ``source_registry.register(name, cls)``.
"""

import logging
import threading
from pathlib import Path

from adapt.downloaders.nexrad import parse_scan_time
from adapt.modules.acquisition.module import AwsNexradDownloader

logger = logging.getLogger(__name__)

__all__ = ["LocalDirectorySource", "SourceRegistry", "source_registry"]


class SourceRegistry:
    """Name → source class. Resolved by the orchestrator from ``config.source``."""

    def __init__(self) -> None:
        self._sources: dict[str, type] = {}

    def register(self, name: str, source_class: type) -> None:
        if name in self._sources:
            raise RuntimeError(f"Source '{name}' is already registered.")
        self._sources[name] = source_class

    def get(self, name: str) -> type:
        if name not in self._sources:
            raise KeyError(f"Source '{name}' is not registered.")
        return self._sources[name]

    def names(self) -> list[str]:
        return list(self._sources)


class LocalDirectorySource(threading.Thread):
    """Acquire files already present in ``config.source_dir``, in chronological order.

    A finite source: once every file is acquired into the store and queued it
    reports complete. Use when the files were downloaded by an external process
    and only need processing.
    """

    def __init__(self, config, result_queue=None, acquire=None) -> None:
        super().__init__(daemon=True, name="LocalDirectorySource")
        if not config.source_dir:
            raise ValueError(
                "source_dir is required for the 'local_directory' source. "
                "Set source_dir in your config to the directory of files to process."
            )
        if acquire is None:
            raise ValueError("LocalDirectorySource requires the acquisition gateway")
        self._dir = Path(config.source_dir)
        self._result_queue = result_queue
        self._acquire = acquire
        self._stop_event = threading.Event()
        self._min_file_size = config.downloader.min_file_size
        self._total = 0
        self._queued = 0
        self._complete = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def stopped(self) -> bool:
        return self._stop_event.is_set()

    def is_historical_complete(self) -> bool:
        return self._complete.is_set()

    def get_historical_progress(self) -> tuple[int, int]:
        return self._queued, self._total

    def run(self) -> None:
        files = sorted(p for p in self._dir.iterdir() if p.is_file())
        files = [p for p in files if p.stat().st_size >= self._min_file_size]
        self._total = len(files)
        for path in files:
            if self.stopped():
                break
            if self._result_queue is not None:
                # Loud failure on an unparseable stamp: the store cannot register
                # a scan without its observation time (no wall-clock substitute).
                scan_time = parse_scan_time(path.name)
                message = self._acquire.acquire_file(
                    path, source_uri=str(path), scan_time=scan_time
                )
                self._result_queue.put(message)
            self._queued += 1
        self._complete.set()
        logger.info(
            "LocalDirectorySource queued %d/%d files from %s", self._queued, self._total, self._dir
        )


source_registry = SourceRegistry()
source_registry.register("aws_nexrad", AwsNexradDownloader)
source_registry.register("local_directory", LocalDirectorySource)
