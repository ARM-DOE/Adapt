# Acquisition Module

**Location**: `src/adapt/modules/acquisition/module.py`

---

## Responsibility

The acquisition module is the **data ingress boundary** of the system. Its sole job is to discover and download raw NEXRAD Level-II files from AWS S3 and deliver local file paths to the rest of the pipeline.

It runs in its own thread, independent of all processing. It has no knowledge of what happens to the files after delivery.

---

## What It Is Not Responsible For

- Parsing or reading file contents — that is the ingest module's job
- Managing the output directory structure — that is the repository's job
- Tracking which files have been processed — that is the file tracker's job
- Retrying failed downloads indefinitely — fail loudly on persistent failure
- Making decisions about which files are scientifically relevant — deliver all files, let the pipeline decide

---

## Interface

The acquisition module is a `threading.Thread` subclass, not a `BaseModule`. It runs concurrently with the processor and communicates via a thread-safe queue.

```
AwsNexradDownloader
├── Inputs (constructor):
│   ├── radar_id: str              # NEXRAD site (e.g., "KDIX")
│   ├── config: InternalConfig     # Mode, timing parameters
│   ├── output_dir: Path           # Where to write downloaded files
│   └── queue: Queue               # Channel to processor thread
│
└── Output (to queue):
    └── {"path": Path, "queued_at": float}   # One dict per file
```

---

## Operating Modes

### Realtime Mode

Polls AWS S3 on a configurable interval (default: 60 seconds) for new files within a rolling time window (default: 60 minutes). Files already seen in a prior poll are not re-enqueued.

This mode runs until externally stopped (Ctrl+C or `orchestrator.stop()`).

### Historical Mode

Fetches the complete list of files for a fixed time range (`start_time` to `end_time`) and enqueues them all. No polling. Terminates when the queue is exhausted.

---

## Thread Lifecycle

```
AwsNexradDownloader.run()
    │
    ├── [ Historical ] enumerate all files in range
    │       │
    │       └── for each file: download → queue.put(path)
    │
    └── [ Realtime ] loop until stopped
            │
            ├── poll S3 for new files in rolling window
            ├── for each new file: download → queue.put(path)
            └── sleep(poll_interval_seconds)
```

The thread signals completion by putting a sentinel value (`None`) into the queue. The processor thread exits its loop when it receives the sentinel.

---

## Connection to the Rest of the System

The acquisition module is the **only component that touches AWS S3**. All other components operate on local files.

```
AWS S3
  ↓
AwsNexradDownloader (thread)
  ↓  queue.put(filepath)
RadarProcessor (thread)
  ↓  queue.get()
IngestModule.run(context)
```

The queue is the only coupling between acquisition and processing. Neither thread knows anything about the other's implementation.

---

## Design Notes

**Why a thread, not a BaseModule?**

The acquisition module has a fundamentally different execution model from processing modules. It runs continuously in the background, driven by time rather than by data availability. Fitting it into the `BaseModule` interface (which runs once per file on demand) would require awkward state management. A thread with a queue is the natural, honest model.

**Why is the queue bounded (size=20)?**

Back-pressure. If the processor is slower than the downloader (e.g., during historical replay of a large archive), the downloader will block rather than accumulating an unbounded queue that consumes memory. This is a deliberate flow-control mechanism.
