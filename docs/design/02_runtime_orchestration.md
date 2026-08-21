# Runtime Orchestration

**Location**: `src/adapt/runtime/`

---

## Responsibility

The runtime layer **coordinates** the system: it starts threads, passes data between them, manages the file processing lifecycle, and gracefully shuts down. It contains no science, no storage logic, and no configuration resolution.

It is the conductor, not the orchestra.

---

## What It Is Not Responsible For

- Processing radar data — that is the module layer's job
- Executing the module graph — that is the execution graph's job
- Storing or reading artifacts — that is the persistence layer's job
- Resolving configuration — that is the configuration layer's job
- Any decisions about cell detection, tracking, or projection parameters

---

## Components

### PipelineOrchestrator (`orchestrator.py`)

The top-level entry point for a pipeline run. Created by the CLI and handed a fully resolved `InternalConfig`.

**Responsibilities**:
- Start and stop the downloader, processor, and plot consumer threads
- Own the inter-thread communication queue
- Log pipeline status periodically
- Handle shutdown signals (Ctrl+C, timeout)
- Close repository connections cleanly on exit

**Thread topology**:

```
Main Thread
    │
    ├── AwsNexradDownloader (daemon thread)
    │       │ queue.put(filepath)
    │       ▼
    ├── RadarProcessor (worker thread)
    │       │
    │       ▼ repository writes
    │
    └── PlotConsumer (daemon thread)
            │ polls repository
            ▼ writes PNG files
```

The queue between downloader and processor has a bounded size (default: 20 items). If the processor is slower than the downloader, the downloader blocks. This prevents unbounded memory growth during historical replay of large archives.

**Lifecycle**:
```
orchestrator.start()
    │
    ├── start downloader thread
    ├── start processor thread
    ├── start plot consumer thread (if visualisation enabled)
    │
    └── main loop:
            ├── sleep 30s
            ├── log status (queue depth, processed count, error count)
            └── check for thread exits or timeout
                    │
                    ▼ (on stop signal or timeout)
orchestrator.stop()
    │
    ├── signal downloader to stop
    ├── wait for processor to finish current file
    ├── give plot consumer 10s to flush
    └── close repository (flush SQLite WAL, close connections)
```

### RadarProcessor (`processor.py`)

The worker thread that executes the processing pipeline for each file. It reads file paths from the queue, runs the execution graph, and writes results to the repository.

**Responsibilities**:
- Pop acquisition messages (`{artifact_id, scan_id, scan_time}`) from the queue
- Skip scans the catalog already marks complete (`scan_is_complete`)
- Maintain the two-frame rolling buffer for projection pairing
- Execute the processing graph via `NexradPipeline`
- Route every output into the store via `StoreOutputRouter`
- Record failures via `mark_scan_failed` and module timings via execution history
- Handle and log per-scan errors without stopping the thread

**Frame pairing logic**:

Projection and analysis require two consecutive frames. The processor manages this:

```
File N arrives
    │
    ├── Run ingest + detection → segmented_ds_N
    ├── Add to frame history [segmented_ds_{N-1}, segmented_ds_N]
    │
    ├── If history has < 2 frames: skip projection, wait for next file
    │
    ├── If time gap between frames > max_time_interval: reset history, skip projection
    │
    └── Run projection + analysis + tracking with:
            segmented_ds  = segmented_ds_N     (current)
            prev_segmented_ds = segmented_ds_{N-1}  (previous)
```

**Why the processor manages frame state, not the projection module?**

The processor has the full context of execution order and timing. The projection module is a stateless, pure function. Keeping frame state in the orchestration layer preserves module purity and makes the projection module independently testable.

### Scan completeness (replaces the old FileProcessingTracker)

There is no separate progress ledger. The catalog's `scans` table is the one
authority: a scan is `complete` when all `REQUIRED_SCAN_PRODUCTS` are linked
(recomputed on every product registration), so restart-skip decisions, the
dashboard timeline, and API reads all agree by construction.

**Methods** (on the collection catalog):
```python
scan_is_complete(run_id, scan_id) → bool  # restart / dedupe decision
mark_scan_failed(run_id, scan_id, err)    # loud per-scan failure record
reset_failed(radar_id)                # Re-queue files stuck in "failed"
get_statistics()                      # Count by status and stage
```

This component has no science in it. It is pure administrative bookkeeping.

---

## Thread Safety Guarantees

1. **Queue**: `queue.Queue` is thread-safe by design. The downloader and processor never share any other mutable state.

2. **Store databases**: SQLite in WAL mode; each thread uses its own connection. No shared mutable Python objects.

3. **Store writes**: the registry serialises writes behind a lock; object commits are staged-then-`os.replace` (atomic). Writes from the processor and reads from consumers are safe.

4. **Module instances**: Module instances (including `TrackingModule`'s internal graph) are owned exclusively by the processor thread. No other thread accesses module state.

---

## Error Handling Policy

The processor catches exceptions per file, logs them with the file path and traceback, and continues to the next file. A single malformed or corrupt NEXRAD file does not stop the pipeline.

The orchestrator does not catch exceptions in the main thread. An unhandled exception in the orchestrator itself propagates up to the CLI, which logs it and exits with a non-zero status code.

---

## Connection to the Rest of the System

```
CLI (cli.py)
    ↓ init_runtime_config() → InternalConfig
    ↓
PipelineOrchestrator
    ↓ start threads
    │
    ├── AwsNexradDownloader (or LocalDirectorySource)
    │       ↓ StoreAcquirer (idempotent raw ingest, mints scan_id)
    │       ↓ queue: {artifact_id, scan_id, scan_time}
    ├── RadarProcessor
    │       ↓ context
    │       ├── NexradPipeline.process_file()
    │       │       ↓ GraphExecutor
    │       │       └── modules: ingest → detection → projection → analysis → tracking
    │       └── StoreOutputRouter
    │               ↓ objects/ + catalog.db + products.db
    └── PlotConsumer (only with --plot-dir)
            ↓ catalog (reads latest complete scan)
            └── RadarPlotter (writes PNG outside the store)
```
