# Document 9 — Production Readiness Assessment

Evaluates readiness for: cloud, HPC, edge, containers, long-running services, and
distributed execution. The honest summary: **Adapt is closest to production-ready for
its most important target — a long-running single-radar site process — and furthest
from ready for cloud/HPC, where the gaps are packaging and persistence, not
architecture.**

## Readiness matrix

| Target | Today | Blocking gaps | Verdict |
|--------|-------|---------------|---------|
| Edge / radar site (long-running, possibly air-gapped) | Realtime mode, resumable tracker, WAL SQLite, graceful shutdown | Supervision story, headless install weight, log rotation, disk retention | **Near** — weeks |
| Container | None: no Dockerfile, no image CI | Packaging only | **Near** — days–weeks |
| Batch reprocessing, single node | Historical mode works end-to-end | Per-(scan,module) resume; chunk CLI | **Near** |
| HPC (many-node reprocessing) | Nothing distributed | Chunk+stitch (Doc 03); SQLite-on-shared-FS hazard; lock-file envs | **Mid** — a quarter |
| Cloud (object storage, autoscale) | Filesystem-only persistence | fsspec boundary; container; metrics | **Mid** |
| Long-running service hardening | Thread liveness checks, status logs | Health/metrics endpoint, memory audit, crash-only design gaps | **Mid** |
| Distributed real-time (multi-radar fleet) | One radar per process (by design) | Fleet supervision + federated catalog only — **keep shared-nothing** | **By design** — don't "fix" |

## Per-environment analysis

### Edge / site deployment (the design center — and the constitution's beneficiary)

What's already right: the whole stack is a file tree + SQLite — works air-gapped;
realtime/historical share one code path; catalog scan completeness gives idempotent
restart; threads are monitored with liveness checks and clean Ctrl+C shutdown.

Gaps:
1. **Supervision.** A site process must survive its host: ship a systemd unit
   (`Restart=on-failure`) and document the warm-restart contract. Today a restart
   re-primes scan history from nothing (first post-restart scan can't run multi-frame
   modules); serialized window state (Doc 03) removes that science gap.
2. **Crash-only discipline.** Graceful shutdown is implemented; *ungraceful* needs an
   audit: WAL gives DB integrity, atomic temp-file renames protect artifacts — good —
   but the tracker can mark stages complete while artifacts from a different phase
   lag. Define and test the invariant "tracker state never claims more than the
   catalog can prove."
3. **Retention.** Nothing bounds disk growth (raw + gridded NetCDF accumulate
   forever). Need a retention policy module: age-out raw files, keep tables; this is
   itself a `post_run`-triggered module, not new infrastructure.
4. **Install weight.** A headless site pulls matplotlib + OpenCV + Tkinter-adjacent
   code today (S9). Extras split fixes it.
5. **Observability without a screen** (below).

### Containers

Pure gap, no architectural blocker: add `Dockerfile` (slim, headless extras),
build/publish in CI alongside the existing PyPI workflow, document volume mounts for
`base_dir` and config injection (env var > config file precedence for the handful of
deployment-varying settings — base_dir, radar, mode — without abandoning
file-as-truth). One process per container; compose/k8s examples for the multi-radar
fleet are documentation, not features.

### Cloud

1. **Persistence is `pathlib`-bound.** The store's objects/catalog assume a local
   filesystem. Fix at the right altitude: an **artifact-store adapter using fsspec**
   inside `persistence/` (the adapters layer is exactly where third-party I/O is
   allowed) — local paths and `s3://` URLs through one interface. Do *not* spread
   fsspec through the codebase; only the repository touches storage (already the
   rule).
2. **Catalog stays SQLite per run** — on the *local* disk of the worker, synced to
   object storage at finalize. Resist "move the catalog to Postgres/Aurora" until a
   real concurrent-writer requirement exists (none does: runs are single-writer by
   design). A federated *read* layer (DuckDB over many run catalogs/Parquet — DuckDB
   is already a dependency used by the API) covers fleet-wide queries without a
   database service.
3. **Realtime ingestion** already polls S3 (`nexradaws`); the source abstraction
   (Doc 02) admits an SQS/EventBridge-driven source later without engine changes.

### HPC batch

1. The unit of distribution is the **time chunk**, not the DAG node (Doc 03):
   `adapt run --start --end` per array task, shared-nothing, each writing its own run
   directory; `adapt stitch` reconciles tracking seams. No MPI, no scheduler
   integration in core — a documented SLURM array example is the deliverable.
2. **Hazard to document loudly:** per-run SQLite on Lustre/NFS is fine *only* because
   each run directory has a single writer process. The README for HPC must say:
   never point two tasks at one run directory; WAL on NFS is where SQLite goes to
   die.
3. **Environment reproducibility:** conda-lock/uv lock files per release (Doc 04
   determinism policy) so a 10,000-job campaign runs one environment.

### Long-running service hardening

1. **Observability.** Today: log lines + a 30s status line + Tkinter dashboard.
   Needed: machine-readable health. Smallest correct thing, honoring "no servers in
   the kernel": the orchestrator already computes status — write it as a heartbeat
   JSON file/table (`status`, queue depth, last scan time, per-module failure
   counts); a trivial optional consumer exposes it as `/healthz` + Prometheus
   metrics. Sites scrape it; nothing changes in-process.
2. **Memory audit.** `_scan_history` keeps full per-scan contexts (entire 3-D + 2-D
   datasets) for `max_history` scans; module instances (projector/tracker) accumulate
   state across the run; matplotlib figures in the plot consumer are a classic leak
   source. Action: a 72-hour soak test with RSS tracking in CI-adjacent tooling
   (integration-labelled), explicit `del`/window-trim of dataset payloads once
   persisted, keeping only what `windowed(n)` state declares (Doc 03 makes this
   structural).
3. **Clock and gap robustness:** the time-gap rule already guards data gaps;
   late/out-of-order file arrival in realtime mode deserves an explicit test (the
   tracker dedupes, but history ordering assumes monotonic arrival).
4. **Thread → process isolation** (Doc 03): a wedged downloader can't take the
   processor with it if they are separate processes meeting at the filesystem +
   catalog; this also makes the GIL irrelevant. Sequenced after the pure
   `process_scan` refactor.

### Distributed execution — the deliberate non-goal

Restated as policy so nobody "fixes" it: **Adapt scales by running more independent
processes over disjoint data (radars × time chunks), never by distributing one
pipeline.** Per-scan compute (seconds) against inter-scan arrival (minutes) means
distributed DAG execution would add coordination cost and nondeterminism (Principle 9)
for zero throughput need. The architectural gaps that *do* matter for fleets are
supervision (k8s/systemd templates) and federated read (DuckDB layer) — both Ring 2.

## Gap list, prioritized

| # | Gap | Severity | Effort | Unlocks |
|---|-----|----------|--------|---------|
| G1 | Headless extras split (`[viz]`, `[gui]`) | High | S | edge, containers |
| G2 | Dockerfile + image CI | High | S | cloud, HPC, reproducible sites |
| G3 | Heartbeat/health file + metrics consumer | High | S | operations of any kind |
| G4 | fsspec artifact-store adapter | High | M | cloud |
| G5 | Serialized window state (warm restart) | Med | M (with Doc 03) | edge robustness, chunk handoff |
| G6 | Chunk + stitch batch CLI | Med | M | HPC campaigns |
| G7 | Retention/cleanup module | Med | S | unattended sites |
| G8 | Soak test + memory audit | Med | S | long-running trust |
| G9 | Lock-file environments per release | Med | S | HPC/site reproducibility |
| G10 | systemd/k8s deployment examples | Low | S | adoption |

## Scoring the recommended posture

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | No new concepts for module authors; ops live in Ring 0/2 |
| Extensibility | High | New storage = fsspec URL; new telemetry = consumer |
| Testability | High | Shared-nothing processes test as single processes |
| Reproducibility | High | Lock files + containers + manifests close the environment loop |
| Operational complexity | Low | Zero mandatory services; SQLite + files everywhere; servers only as optional consumers |
| Long-term maintainability | High | Scale-out story (more processes) doesn't fork the execution model |
