# Document 1 — Architecture Vision

## What Adapt is today, honestly stated

Adapt today is a **NEXRAD convective-cell tracking pipeline with a platform skeleton**.
The skeleton — declared-IO modules, auto-wired DAG, contract checks at edges, a single
artifact repository, import-linter-enforced layering — is genuinely platform-shaped.
The flesh is single-domain: one instrument (NEXRAD Level-II), one science workflow
(grid → segment → project → analyze → track), one runtime topology (two threads and a
queue), domain nouns inside the kernel (`nexrad_file` as a context key,
`config.downloader.radar`, `"reflectivity"` hardcoded in `contracts/grid.py`).

That is not a criticism of the work done — it is the correct way to have built v0:
extract the platform from a working workflow rather than design a platform in the
abstract. The vision question is what to generalize next and, just as importantly,
what **not** to generalize.

## Foundational position — with one amendment

The constitution states: *Adapt is a scientific workflow platform; radar is a plugin.*
Accepted, with a deliberate narrowing:

> Adapt is a **domain-neutral platform for streaming, provenance-tracked scientific
> workflows over time-ordered observational data**. It is not a general workflow engine.

The amendment matters because "scientific workflow platform" without qualification is
the gravestone of many projects: it points at competing with Airflow, Prefect, Dask,
Pegasus, and Snakemake — mature, funded, general systems. Adapt cannot and should not
win that fight. What none of those systems provide, and what Adapt's existing design is
already unusually good at, is the combination of:

- **scan-by-scan streaming semantics** (a rolling window of stateful history — tracking
  needs scan N−1; generic DAG engines model stateless tasks);
- **scientific provenance as a first-class kernel service** (not bolted-on logging);
- **typed scientific data contracts at every edge** (CF-aware datasets and tables, not
  opaque blobs);
- **identical code paths for real-time and archival processing** (a fold over a scan
  stream, where only the source differs).

That is the niche. The kernel should be domain-neutral *within* that niche. Generalizing
beyond it (arbitrary triggers, cron scheduling, distributed task brokering) is scope
failure, not ambition.

## The three rings

```
┌────────────────────────────────────────────────────────────┐
│ Ring 2 — Applications & Consumers                          │
│   dashboards, adaptive-scan controllers, web APIs,         │
│   notebooks, exporters to external systems                 │
│   (consume Ring 0 data via the read API only)              │
├────────────────────────────────────────────────────────────┤
│ Ring 1 — Domain Distributions (plugins)                    │
│   adapt-radar  (NEXRAD/ARM ingest, segmentation, tracking) │
│   adapt-lma    (lightning mapping)                         │
│   adapt-ml     (model-backed detectors/classifiers)        │
│   adapt-sat    (future: geostationary satellite)           │
│   third-party extensions (any institution, any repo)       │
├────────────────────────────────────────────────────────────┤
│ Ring 0 — Kernel (small, stable, boring)                    │
│   contracts   — data specs, module interface, versioning   │
│   execution   — DAG compile + run, module registry         │
│   pipeline    — workflow definitions, triggers, policies   │
│   provenance  — run manifest, lineage, environment capture │
│   config      — resolution, validation, freezing           │
│   persistence — artifact store interface + catalog         │
└────────────────────────────────────────────────────────────┘
```

Ring 0 is the only thing the core team must keep stable for a decade. Today's
`src/adapt` mixes all three rings in one package: `consumers/live` (a Tkinter dashboard)
and `visualization/` are Ring 2; `modules/*` are Ring 1; the rest approximates Ring 0.
The 5-year goal is that the rings are separate installable packages and the kernel has
**zero** scientific dependencies — no Py-ART, no OpenCV, no matplotlib (today all of
these are mandatory dependencies in `pyproject.toml`, which violates Principle 1).

## What Adapt should be in 5–10 years

- **Year 1–2:** One-file plugin modules; provenance mandatory; contracts v1 frozen;
  `adapt-core` + `adapt-radar` package split. Still one institution's project.
- **Year 3–5:** Multiple instrument distributions maintained by different groups;
  a public extension registry (a curated list, not infrastructure); operational
  deployments at ARM sites driving adaptive scanning; archival reprocessing campaigns
  run as chunked batch jobs on HPC.
- **Year 5–10:** Adapt is the boring, trusted substrate — the thing reviewers ask for
  ("was this produced with a tracked Adapt run?"). The kernel changes a few times a
  year. The science churns weekly in plugins without core involvement. Success looks
  like the kernel team being *bored*.

## Key non-functional requirements (ranked)

1. **Determinism.** Identical inputs + config + code version ⇒ bit-identical outputs.
   Non-negotiable (Principle 9). Constrains all parallelism design.
2. **Traceability.** Every artifact answers what/when/version/inputs/config/environment
   with zero user effort (Principle 5). Today: fails — see Document 04.
3. **Contributor time-to-first-module < 1 hour** without reading kernel code
   (Principle 6). Today: fails — see Document 07.
4. **Single-machine first.** A laptop must run the full real-time pipeline for one
   radar. Scale-out is achieved by running many independent processes
   (shared-nothing), never by making one process distributed.
5. **Kernel API stability.** Public surface = `contracts` + module interface +
   read API. SemVer, two-minor-release deprecation (Document 10).
6. **Throughput, last.** A NEXRAD volume arrives every ~4–7 minutes; current
   processing is seconds per scan. Performance is not the binding constraint and
   must not drive design.

## Architectural boundaries (the lines that must not blur)

| Boundary | Rule | Enforcement |
|----------|------|-------------|
| Kernel ↔ science | Kernel never imports a science module; science imports only `contracts` + `utils` | `.importlinter` + `tests/test_architecture.py` (already in place — keep) |
| Science ↔ I/O | Modules never touch filesystem, network, or database; the runtime injects data and persists declared outputs | import-linter contract 3 (in place); needs closing of the loophole where `repository` is passed in context |
| Data conversion | External formats convert to the canonical internal model exactly once, at ingest; everything downstream speaks one dialect | Contracts v1 (Document 05) |
| Read ↔ write | Consumers (Ring 2) read via `adapt.api` only; only the runtime writes | Done: `StoreClient` is the only consumer surface, enforced by a fitness test |
| Core ↔ plugins | Adding capability = new package, never a core edit | Entry-point discovery (Document 02) |

## Trade-offs, stated plainly

**Generality vs. delivery.** Splitting packages and neutralizing the kernel costs ~1–2
quarters with no new science. Deferring it costs more later: every month, more code is
written against the NEXRAD-flavored kernel. Recommendation: do the *naming and
dependency* neutralization now (cheap), the *package split* at the first external
contributor or second instrument (whichever comes first).

**Plugin indirection vs. debuggability.** Registries and entry points make "where is
this code?" harder than a direct import. Mitigation: `adapt module list` must show
name → package → file path → version for every discovered module, and the compiled
pipeline must be printable (`adapt pipeline show`). Indirection without introspection
tooling is how platforms become hated.

**Reproducibility vs. performance.** Content hashing inputs, snapshotting environments,
and validating contracts at every edge cost CPU. Accepted per Principle 9 — with one
pragmatic valve: edge validation can be sampled in operational mode but is always-on in
CI and reprocessing. Provenance capture is never optional.

**Small core vs. convenience.** Scientists will ask for helpers in the kernel ("just add
a plotting util"). Every such addition makes the kernel less boring. The release valve
is a `contrib` distribution with lower stability guarantees — convenience lives there,
not in Ring 0.

**Buy vs. build for orchestration.** Rejected buying (Airflow/Prefect/Dask) for the
kernel: they bring servers, schedulers, and nondeterminism for a problem (8–50 node
in-process DAG per scan) that 120 lines of code already solve. Accepted: the batch
layer must *compose with* external schedulers (a chunked CLI that a SLURM array or
Argo workflow can call). Build the small thing, rent the big thing.

## Scoring this vision

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | One-hour module path; rings tell contributors exactly where code goes |
| Extensibility | High | Everything outside Ring 0 is a plugin by construction |
| Testability | High | Kernel testable with synthetic modules; science testable without kernel |
| Reproducibility | High | Provenance is a Ring 0 service, not a plugin |
| Operational complexity | Low–Med | Shared-nothing processes; no new servers introduced |
| Long-term maintainability | High | The decade-stable surface is deliberately tiny |
