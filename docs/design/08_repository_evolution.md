# Repository Evolution and Experiment Variants

**Status**: Phases 1–5 implemented (2026-08) as the data store — see
[03_persistence.md](03_persistence.md) for the as-built layer. Scan identity,
the `registry.db` + per-collection `catalog.db`/`products.db`/`objects/`
layout, `artifact_lineage`, catalog-only discovery through `StoreClient`, and
module-owned product tables are live; the old layout is deleted (clean break,
no compatibility code). Experiments/variants, checkpoints, and silver/gold
materialisation (phases 6–10) have not started.

**Deviations from this plan as built**:
- Table schemas freeze on the **first written frame** (dual snapshots in
  `products.db` + `catalog.db`, validated by fingerprint) rather than
  hand-authored contract files; novel columns are rejected, `INTEGER↔REAL` is
  the one dtype compatibility, and there is no `ALTER` or migration machinery.
- Table names are plain (`cells_by_scan`, `cell_stats`), not
  `module__` namespaced; ownership is enforced by the writer
  (`owner_module` in `table_schemas`), not by naming.
- The two-database commit protocol is simpler than the ledger described
  below: scan availability is the catalog's `scans.status`, recomputed from
  `scan_products` links against `REQUIRED_SCAN_PRODUCTS` on every product
  registration. Readers see a scan only when complete.
- `lake/` (silver/gold) directories are not created — silver is deferred
  entirely; `adapt init` creates exactly `registry.db`, `logs/`,
  `collections/`.

**Related documents**: [Persistence Layer](03_persistence.md), [Execution Graph](01_execution_graph.md), [Runtime Orchestration](02_runtime_orchestration.md), [Consumer API](05_consumer_api.md), and [Contracts and Boundaries](06_contracts_and_boundaries.md).

---

## Purpose

This document defines the next repository architecture for Adapt. It replaces the current NEXRAD-shaped, date-directory-oriented persistence layout with a source-neutral repository that supports:

- live, relational module outputs;
- provenance with any number of upstream parents;
- module ownership of tables;
- restart and checkpoint recovery;
- experiment variants that reuse scientifically identical upstream work; and
- later materialisation of validated analytical products as Parquet.

This is a clean-break design. There are no external repository users at the time of writing, so existing test repositories and development outputs may be deleted and recreated. Compatibility code for the old layout must not be retained.

---

## Decisions

1. A collection has two SQLite databases: `catalog.db` and `products.db`.
2. `catalog.db` is metadata only. It does not contain scientific module result rows.
3. `products.db` contains the live, bronze, relational tables owned by pipeline modules.
4. A module may own many tables, but exactly one module owns and writes each table.
5. Other modules depend on published product contracts, never on a producer's physical table name or path.
6. Live relational output is written to SQLite. Parquet is reserved for intentional, validated silver/gold analytical products; it is not the live write-ahead log.
7. NetCDF (or a future equivalent) remains appropriate for multidimensional scientific arrays. File-backed objects are discovered exclusively through the catalog.
8. The repository uses UTC observation times and stable IDs. Directory names must not encode NEXRAD assumptions, local time zones, or run-start dates.
9. Reuse is allowed only for an exact, declared scientific derivation. It is never inferred from a filename, module name, or run ID.

---

## Scan identity (implemented — phase 1)

Decision 8's "stable IDs" are realized as a per-scan **`scan_id`**:

- **Mint**: `sha256(raw_source_file_bytes).hexdigest()[:16]` — content identity,
  independent of filenames, clocks, and runs. Externally verifiable with
  `sha256sum <raw file> | cut -c1-16`. Implemented as the pure function
  `adapt.utils.identity.scan_id_from_bytes`.
- **Owner**: minted exactly once at the source boundary (acquisition after
  download; the local-directory replay source at enqueue). The queue message
  contract is `{path, scan_id, scan_time, queued_at}`; the processor rejects
  messages without identity and never re-derives it.
- **Travels**: queue → processor (observability binds to it) → `PersistenceMeta`
  → every per-scan row (`cells_by_scan`, `cell_events` source/target,
  derived module tables), every catalog `items` row, the `scans` registry
  (`PRIMARY KEY (run_id, scan_id)` — the same volume in two runs is the same
  scan_id under different runs), and the artifact's own attrs
  (`ds.attrs["scan_id"]`, `ds.attrs["scan_time"]`). Consumers key on it —
  never on filenames, never on the dataset `time` coordinate, never within
  tolerance windows.
- **Timestamps are metadata**: `scan_time` (canonical `to_scan_iso` string)
  orders and labels scans; optional `start_time`/`end_time` are per-source
  coverage metadata and may be absent. No timestamp is a join key. The
  scan-time-format fitness test was re-worded accordingly; the evidence for
  that contract change is the pre-identity dashboard failure class
  (silent wall-clock substitution in ingest, `+00:00` vs `Z` string divergence
  between `items` and tracking tables, and ±60/±90 s tolerance joins in
  consumers).
- **Clean break**: pre-identity repositories are not migrated; readers and the
  tracking store fail loudly with "recreate" guidance.

---

## Source-neutral layout

```text
{repository_root}/
├── registry.db
├── logs/
└── collections/
    └── {collection_id}/
        ├── catalog.db
        ├── products.db
        ├── objects/
        │   └── {artifact_id}.{suffix}
        └── lake/
            ├── silver/
            └── gold/
```

A **collection** is a coherent data stream or domain. It may be a NEXRAD radar such as `KDIX`, an MRMS domain, a GOES product/region, or another future source. Its source type is metadata (`source_kind`), not a directory convention.

`objects/` stores immutable file-backed artifacts by artifact ID. Original filenames, source URIs, observation times, and format metadata are catalog fields. No API consumer constructs an object path.

Date partitioning is not a live repository rule. If a future silver/gold Parquet product needs partitions, its materialiser may partition by each record's UTC observation date and query workload. A run crossing midnight therefore requires no special case.

---

## Database responsibilities

### `registry.db`

Repository-wide operational metadata:

- registered collections and source metadata;
- pipeline runs, executions, experiment groups, and variants;
- code/configuration provenance;
- repository and schema migration history;
- cross-collection status and health summaries.

### `catalog.db`

Per-collection discovery and provenance metadata:

- `scans` — stable scan identity and UTC observation time;
- `artifacts` — location, format, checksum, producer, status, schema version, and scan/run association;
- `artifact_lineage` — an edge table for parent-child relationships;
- `module_executions` — per-module, per-scan execution and retry status;
- `published_products` and `table_contracts` — discoverable product/table contracts and ownership;
- `checkpoints` — state snapshot location and compatible derivation identity;
- failure, warning, and reconciliation records.

`artifact_lineage` replaces JSON or comma-separated parent IDs:

```text
child_artifact_id
parent_artifact_id
relationship_type

PRIMARY KEY (child_artifact_id, parent_artifact_id, relationship_type)
```

It supports one, many, or zero parents without an artificial limit and makes provenance traversal queryable.

### `products.db`

Live bronze tables, using namespaced names that communicate ownership:

```text
tracking__tracks
tracking__cell_observations
tracking__cell_events
analysis__cell_metrics
selection__recommendations
```

The exact tables are defined by module contracts. A table contract declares the owning module, published product name/version, explicit columns and types, primary key, foreign-key requirements, indexes, mutation policy, and retention policy.

Schemas are never inferred from DataFrames and writes never silently add columns. A structural change requires an explicit, reviewed migration and a new schema version.

---

## Module ownership and contracts

The repository must reinforce, not weaken, the existing module boundary:

- Modules remain free of filesystem paths and persistence implementation details.
- The runtime validates a module's input and output contracts.
- The repository writer persists only tables and artifacts declared by that module's ownership contract.
- A module cannot write another module's table.
- A downstream module requests a published domain product, for example `tracked_cell_observations:v1`, rather than `tracking__cell_observations`.

During one pipeline invocation, downstream nodes normally receive validated upstream values in memory. They must not re-read SQLite merely because the upstream product has been persisted. Repository reads are for independent pipelines, post-processing, restart/recovery, dashboards, APIs, and historical analysis.

---

## Live data and analytical lake

```text
module outputs (live bronze SQLite)
        ↓ validate, normalise, enrich, join
silver Parquet analytical products
        ↓ aggregate or publish
gold Parquet/API-ready products
```

SQLite is the live relational store because it supports indexed row writes, transactions, foreign-key checks within `products.db`, and tracking-style lifecycle queries. Parquet is not a replacement for this role: it is immutable and efficient for large columnar analysis, but is poorly suited to frequent small writes, updates, and relational integrity.

---

## Catalog and products commit protocol

The databases are deliberately separate, so SQLite cannot make a WAL transaction atomically span both files. The persistence layer must use a durable, recoverable protocol:

1. Create a `module_execution` in `catalog.db` with status `writing` and a unique `write_id`.
2. Write all tables owned by that module for the scan in one `products.db` transaction, including a durable commit-ledger row carrying the same `write_id`.
3. Register products, artifacts, and lineage in `catalog.db`.
4. Mark the execution `complete`.
5. On startup, reconcile incomplete `writing` executions: complete catalog registration from a committed ledger entry, or record a failed execution.

No reader may treat a product as available until catalog status is `complete`. This is the required crash-recovery rule.

---

## Experiments, variants, and reuse

### Problem

Scientists must be able to compare segmenters, trackers, and parameterisations against the same source scans without re-downloading or recomputing scientifically identical upstream stages.

### Identity model

- **Experiment**: a named comparison group and common input selection.
- **Variant**: a named configuration or module choice within an experiment.
- **Execution**: one launch, retry, or resume attempt.
- **Derivation**: an immutable scientific computation and its output products.

A derivation key is computed from:

- published product contract and schema version;
- module implementation/version fingerprint;
- normalised module configuration;
- ordered input artifact or derivation IDs;
- source-content checksum or immutable source version; and
- every declared runtime setting that affects scientific output.

Only an exact derivation-key match is reusable. Cache decisions must be explainable through `adapt plan`; the system must report why each node is reused or recomputed.

### Union-DAG execution

An experiment planner expands all variants, builds their union DAG, and deduplicates nodes with identical derivation keys:

```text
raw source → ingest → grid → segmentation → analysis
                                           ├── tracker A
                                           └── tracker B
```

When segmentation differs, branches begin at segmentation and every dependent downstream computation receives the appropriate branch. Shared outputs are referenced as lineage parents, never copied.

The current graph has one shared context and output-key vocabulary. Variant support therefore requires distinct graph-node instance IDs and branch-safe output ports, for example `tracking@baseline` and `tracking@optical-flow`. It must not allow two variant modules to overwrite the same context key.

### Checkpoint and restart

The planner tracks completion per derivation, module, and scan; it replaces the current source-specific broad-stage tracker as the authority for restart decisions.

Stateful modules, especially tracking, must write a versioned state checkpoint after each successfully committed scan. A checkpoint is reusable only when its module/version/configuration fingerprint and upstream derivation lineage match. Restart restores the latest compatible checkpoint and computes only missing or failed work.

Suggested user-facing workflow:

```bash
adapt plan experiment.yaml
adapt run experiment.yaml --reuse auto
adapt resume <execution-id>
adapt compare <experiment-id>
```

### Comparison products

Provenance makes products discoverable side by side, but it does not prove that cells with different labels or tracker IDs are equivalent. A dedicated comparison/evaluation module must publish its own matching and metric products when segmentation variants are compared. For example, it may own `evaluation__cell_matches` and `evaluation__variant_metrics`.

---

## Implementation sequence

Each phase must leave the pipeline, public reader API, GUI, and full relevant test suite working. Tests are changed only when their asserted behaviour is intentionally replaced.

1. ✅ Document source-neutral IDs, ownership contracts, and the clean-break migration policy.
2. ✅ Introduce `registry.db`, per-collection `catalog.db`, and `products.db`; update all readers to discover products through the catalog only (`StoreClient`).
3. ✅ Replace artifact parent JSON with `artifact_lineage` and replace fixed product columns on scans with generic artifact relations (`scan_products` + completeness recompute).
4. ✅ Remove schema inference and automatic `ALTER TABLE` writes (as first-frame freeze + `SchemaLedger`, not migration files — see deviations above).
5. ✅ Move live tracking and analysis relational outputs into module-owned `products.db` tables (`TrackStore`, `TableWriter`).
6. Add node-completion persistence and the catalog/products commit ledger with reconciliation (partially: `run_modules` aggregates + scan completeness; no write-ahead ledger).
7. Add experiment, variant, execution, and derivation identities; implement union-DAG planning and exact reuse.
8. Add state checkpoint contracts and derivation-aware restart.
9. Implement contract-based reader/API and GUI comparison views.
10. Add independent silver/gold materialisation pipelines for validated Parquet products.

Required regression coverage includes ownership enforcement, explicit schema migration, unlimited-parent lineage, exact reuse, changed-config/code/input invalidation, branch isolation, failed-write reconciliation, stateful restart, UTC cross-day processing, and GUI/API access without filesystem inspection.

---

## PostgreSQL boundary

SQLite remains the intended store for this phase: a local per-collection pipeline has a controlled writer and benefits from simple deployment, WAL reads, and no database service. PostgreSQL becomes appropriate only when Adapt needs multiple independent concurrent writers, a shared network service, high availability, or centralized multi-host operations. Parquet remains complementary rather than a database replacement.
