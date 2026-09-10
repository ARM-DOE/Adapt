# Document 3 — Pipeline Architecture

Principle 4: users consume workflows, not algorithms. This document designs the
workflow system: DAGs, linear pipelines, branching, conditional execution, streaming,
real-time, and batch — and an execution blueprint that keeps determinism sacred.

## Current state, assessed

The in-process engine is a 200-line micro-DAG: `GraphBuilder` wires nodes by matching
declared output keys to input keys; `GraphExecutor` runs a topological loop, validating
contract callables at each edge. This is good. The problems are in everything wrapped
around it:

1. **The DAG is not actually in charge.** `RadarProcessor.__init__` partitions modules
   into *executor groups by `required_history`* and *phases* (`pipeline_phase != 3`,
   `== 3`, `POSTPROCESS_PHASE = 4`), then runs the groups in a hand-coded sequence with
   hand-coded conditions (`_validate_time_gap`, `_should_run_enrichment`). The elegant
   declared-IO wiring decides ordering only *within* a group; the real workflow
   topology lives as magic integers in module declarations plus `if` statements in the
   runtime. A contributor cannot learn the workflow by reading any single artifact.
2. **Conditional execution is hardcoded.** "Enrichment runs only when tracking
   committed `cell_uid`" is an `if` in `processor.py`, not a property of the enrichment
   module. Adding a differently-conditioned module means editing the runtime
   (Principle 7 violation).
3. **Two assembly paths, one dead.** `NexradPipeline` in
   `execution/pipeline_builder.py` is referenced by nothing, ignores phases and
   history grouping, and would assemble a *different* pipeline than the processor.
   Delete it (Doc 08, S3).
4. **No notion of a named workflow.** There is exactly one pipeline, implicit in
   "whatever modules are registered minus `--not`". Users cannot define, share, or
   version a workflow — which is precisely what Principle 4 says they consume.

## Target architecture

### One workflow model: a fold over a scan stream

Every Adapt execution mode is the same computation:

```
state₀ = {}
for scan in source:                       # the only thing that varies by mode
    artifacts, state = process_scan(scan, state, plan)
    persist(artifacts)                    # framework, not modules
```

- **Real-time** = source yields scans as they arrive (poll AWS, watch a directory).
- **Batch/archival** = source yields a finite, ordered list. Identical code path —
  this property already exists in Adapt's realtime/historical modes and is one of its
  most valuable invariants. Preserve it absolutely.
- **Streaming** = the loop itself; no separate machinery needed at radar data rates
  (one volume per ~4–7 min). Do not introduce Kafka-shaped infrastructure for a
  problem with seconds of compute and minutes between events.

`process_scan` must become a **pure function of (scan, state, plan, config)** — today
its equivalent is smeared across `RadarProcessor.process_file` and mutable instance
attributes (`_scan_history`, module instance state inside `ProjectionModule`/
`TrackModule`). Purity here is what unlocks everything else: per-scan testing without
threads, chunked batch parallelism, and crash recovery from persisted state.

### The ExecutionPlan: compile once, run per scan

Replace phase integers and history grouping with a compiled plan:

```
plan = compile(modules, config)
# plan knows, per node:
#   - dependencies (from declared IO — as today)
#   - trigger:    every_scan | when(predicate_on_context) | post_run
#   - state:      stateless | windowed(n)   (replaces required_history)
#   - on_error:   halt_run | skip_scan      (contract violations always halt)
#   - persistence: declared output specs    (replaces _save_results hardcoding)
```

- **`trigger: when(...)`** absorbs the hardcoded conditions. The time-gap rule becomes
  a declared predicate on windowed state ("previous scan within N minutes"), owned by
  the modules that need it. The enrichment rule ("tracked cells with `cell_uid`
  present and non-empty") becomes `when("tracked_cells")` with its existing contract.
  A skipped node's downstream nodes skip too, recorded in provenance as *skipped*,
  not absent.
- **`state: windowed(n)`** replaces `required_history` *and* the processor's manual
  `_scan_history` list. The framework maintains the window, passes it read-only into
  the node, and — critically — can serialize it, which gives warm restarts (Doc 09)
  and chunk-boundary handoff (below).
- **Phases die.** Phase 0–2 ordering is already derivable from IO dependencies.
  Phase 3 ("after persistence") becomes `trigger: post_run` — the only genuinely
  non-dataflow ordering in the system, so it gets one explicit named concept instead
  of an integer. `POSTPROCESS_PHASE = 4` becomes a different *plan* (see next), not a
  different module kind.

### Named pipelines as data

A workflow is a small, versioned, shareable YAML document — not code:

```yaml
# pipelines/nexrad_tracking.yaml
pipeline: nexrad_tracking
version: 2
modules: [ingest, detection, projection, analysis, tracking, cell_volume_stats]
```

`adapt run --pipeline nexrad_tracking.yaml`. The existing `modules:` allowlist in user
config is 80% of this already; the change is making it a named, versioned artifact that
is hashed into the run manifest (Doc 04). The current `PostProcessor` stops being a
separate framework class with its own YAML and module-loading function (today it
duplicates `_ensure_modules_registered`) and becomes: the same engine, running a
different pipeline document whose source is "scans already in a repository". One
engine, N pipeline documents. Branching and linear pipelines need no new features —
they are just DAGs with the shape users declare by module selection.

## Engine decisions

### Scheduling and parallelism

- **Within a scan:** keep sequential execution as the reference. The topological loop
  may later run ready nodes in a thread pool, but only after the determinism audit
  (floating-point reductions, OpenCV thread behavior) and behind a config flag that
  defaults off. Replace the O(n²) ready-scan with Kahn's algorithm when node counts
  grow — a 30-line change, not urgent at 8 nodes, necessary before "hundreds of
  modules."
- **Across scans (batch):** the scale-out mechanism is **time-chunking with stitching**:
  split the archive into chunks, run independent shared-nothing processes per chunk
  (each writes its own run catalog), then stitch stateful seams — for tracking, re-run
  the linker over the overlap window and reconcile `cell_uid`s, the same problem TITAN
  reprocessing solves. This composes with any outer scheduler: GNU parallel, SLURM
  arrays, Argo, Prefect. Adapt provides `adapt run --start --end` (exists) and
  `adapt stitch <run...>` (new); it does **not** provide a distributed scheduler.
- **Real-time:** today's two threads (source → queue → processor) are adequate and
  proven. Long-term, prefer two *processes* (source and engine) communicating through
  the filesystem + catalog, which removes the GIL, isolates crashes, and makes the
  dashboard's poll-the-repository pattern the universal IPC. Do not add more threads.

### Failure handling and recovery

| Failure | Policy | Today | Target |
|---------|--------|-------|--------|
| Contract violation | Halt the run; the pipeline is wrong | ✓ (`ContractViolation` stops processor) | unchanged — this is correct and rare in production |
| Module raises on one scan | Record, skip scan, continue | ✓ but coarse (whole-file try/except, error string in tracker) | per-node error recorded with traceback artifact; downstream skipped; scan marked `failed:<node>` |
| Process crash / restart | Resume without reprocessing | ✓ per-scan via catalog scan completeness (`scan_is_complete`) | + serialized window state ⇒ warm resume without re-priming history |
| Partial DAG (some nodes failed for a scan) | Re-run only what's missing | ✗ (per-file granularity only) | per-(scan, node) completion in the tracker; idempotent persistence (already upsert-based) makes re-runs safe |

### Caching

Batch reprocessing wants "don't recompute what hasn't changed." Key cache entries by
`(input artifact ids, module name+version, config-section hash, contract version)` —
all of which exist once Doc 04 lands. Scope: **opt-in, batch mode only**. A cache in
the real-time path is complexity with no payoff (every scan is new). This is
deliberately Phase-5 work; correctness and provenance come first (Principle 9).

### External engines — evaluated and rejected for the kernel

| Option | Why not |
|--------|---------|
| Airflow / Prefect / Dagster | Server + scheduler + DB for an in-process, per-scan DAG; tasks are stateless by design — the rolling window becomes contortion; operational complexity criterion fails outright |
| Dask delayed/futures | Attractive for batch fan-out, but nondeterministic scheduling, cluster lifecycle, and serialization of stateful modules buy little over shared-nothing chunk processes |
| Snakemake / Nextflow | File-centric batch DAGs; no streaming/real-time story; would split the codebase into two execution models, violating the one-fold invariant |

The consistent reason: Adapt's unit of orchestration is *one scan through a small DAG
with windowed state*, and its scale-out unit is *independent time chunks*. Both are
trivially served by simple code; all external engines tax the common case to serve a
case Adapt doesn't have. Compose with them at the CLI boundary instead.

## Blueprint

```
                       ┌─────────────────────────────────────────┐
                       │ Pipeline document (YAML, versioned)     │
                       └───────────────┬─────────────────────────┘
                                       ▼
 entry-point discovery ──► ModuleRegistry ──► compile() ──► ExecutionPlan
                                                              │ (nodes, edges,
                                                              │  triggers, state,
                                                              │  persistence specs)
        Source (module, scheduled by runtime)                 ▼
        realtime poll / local dir / repository ──► scan ──► process_scan(scan, state, plan)
                                                              │
                                              ┌───────────────┼───────────────┐
                                              ▼               ▼               ▼
                                        artifacts        new state       provenance
                                              │               │           records
                                              ▼               ▼               ▼
                                        ArtifactStore    state store     run catalog
                                        (declared specs) (serialized)   (lineage, Doc 04)
```

## Migration order

1. Delete `NexradPipeline`. Make selected-module import failures fatal. (days)
2. Extract `process_scan` as a pure function; move `_scan_history` into an explicit,
   serializable `WindowState` owned by the engine. (the keystone refactor)
3. Replace `_save_results` hardcoding with declared persistence specs (Doc 08, S1).
4. Replace `pipeline_phase` with `trigger`; fold `PostProcessor` into "same engine,
   different pipeline document."
5. Pipeline YAML documents + manifest hashing; then chunked batch + stitch.

## Scoring

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | Workflow visible in one document; conditions declared on modules, not buried in runtime |
| Extensibility | High | New trigger/persistence needs = new declarations, not runtime edits |
| Testability | High | `process_scan` pure ⇒ table-driven tests, no threads or queues |
| Reproducibility | High | Plan + pipeline doc are hashable inputs to the run manifest |
| Operational complexity | Low | No new services; scale-out is processes + files |
| Long-term maintainability | High | One engine, one execution model, phases and dead paths removed |
