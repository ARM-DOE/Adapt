# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""HDF5 diagnostics are silenced through h5py's supported control, on the I/O thread.

We cannot reliably force libhdf5 to print a diagnostic in every environment, so the
behaviour under test is the contract: the helper invokes h5py's official silence,
is idempotent, and the processor applies it at worker-thread entry (HDF5 error
stacks are thread-local, so a single import-time call would miss the worker).
"""

import queue
import random
from datetime import UTC, datetime

import pytest

from adapt.runtime import diagnostics
from adapt.runtime.observability import ObsSettings, build_observability
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def test_silence_hdf5_errors_calls_h5py_supported_control(monkeypatch):
    calls = []
    monkeypatch.setattr(diagnostics.h5py._errors, "silence_errors", lambda: calls.append(True))

    diagnostics.silence_hdf5_errors()
    diagnostics.silence_hdf5_errors()  # idempotent — safe to call per thread entry

    assert calls == [True, True]


def _obs():
    return build_observability(
        ObsSettings(),
        clock=lambda: 0.0,
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def test_processor_silences_hdf5_at_worker_thread_entry(
    monkeypatch, pipeline_config, pipeline_output_dirs, test_repository
):
    import adapt.runtime.processor as processor_module

    called = []
    monkeypatch.setattr(processor_module, "silence_hdf5_errors", lambda: called.append(True))

    proc = RadarProcessor(
        queue.Queue(),
        pipeline_config,
        pipeline_output_dirs,
        repository=test_repository,
        observability=_obs(),
    )
    proc.stop()  # so _run_loop exits immediately; run() still hits the thread-entry call
    proc.run()

    assert called == [True]  # silenced once, at the top of the worker thread
