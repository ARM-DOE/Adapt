# Adapt Design Documentation

Design-focused documentation for the Adapt geospatial sensor analysis platform.
Each document covers a layer or component: its single responsibility, what it explicitly does not do, how it connects to other parts of the system, and where it can be extended.

---

## Documents

| # | Document | What It Covers |
|---|----------|----------------|
| 00 | [Architecture Overview](00_architecture_overview.md) | System layers, data flow, dependency rules, SOLID principles applied |
| 01 | [Execution Graph](01_execution_graph.md) | DAG construction, topological execution, module registry, context dict protocol |
| 02 | [Runtime Orchestration](02_runtime_orchestration.md) | Thread coordination, frame pairing, scan completeness, graceful shutdown |
| 03 | [Persistence Layer](03_persistence.md) | The data store: layout, catalogs, frozen tables, objects, lineage |
| 04 | [Configuration System](04_configuration.md) | Three-tier resolution, expert defaults, frozen InternalConfig |
| 05 | [Consumer API](05_consumer_api.md) | StoreClient — scans, rasters, tracks, typed filters, read-only SQL |
| 06 | [Contracts and Boundaries](06_contracts_and_boundaries.md) | What contracts are, how they are enforced, what they do not validate |
| 07 | [Vision and Extensibility](07_vision_and_extensibility.md) | Multi-sensor roadmap, cloud deployment, invariants that must be preserved |
| 08 | [Repository Evolution and Experiment Variants](08_repository_evolution.md) | Source-neutral repository, catalog/products split, live tables, reusable experiment variants, checkpoints |

## Decision Records

| Date | Document | Decision |
|------|----------|----------|
| 2026-06-11 | [cell-projected lightning association](2026-06-11_cell_projected_lightning_association.md) | Minute-mask association of xLMA flashes to tracked cells |
| 2026-07-04 | [target selection engine](2026-07-04_target_selection_engine.md) | TSE consumer: gates, scoring, replay |
| 2026-08-21 | [data store redesign](2026-08-21_data_store_redesign.md) | Store layout, frozen schemas, StoreClient query surface, uniform modules — implements 08 phases 1–5 |

## Module Documents

| Module | Document | Responsibility |
|--------|----------|----------------|
| Acquisition | [modules/acquisition.md](modules/acquisition.md) | Download raw files from AWS S3 |
| Ingest | [modules/ingest.md](modules/ingest.md) | Translate polar radar format to Cartesian grid |
| Detection | [modules/detection.md](modules/detection.md) | Segment cells from reflectivity field |
| Projection | [modules/projection.md](modules/projection.md) | Estimate motion, extrapolate cell positions |
| Analysis | [modules/analysis.md](modules/analysis.md) | Compute per-cell physical statistics |
| Tracking | [modules/tracking.md](modules/tracking.md) | Link cells across scans, assign stable UIDs |

---

## Key Concepts to Understand First

If you are new to this codebase, read in this order:

1. **[Architecture Overview](00_architecture_overview.md)** — understand the layers and why they are separated
2. **[Execution Graph](01_execution_graph.md)** — understand how modules connect without importing each other
3. **[Contracts and Boundaries](06_contracts_and_boundaries.md)** — understand how correctness is enforced between stages
4. Then read the module documents for whichever domain you are working in

---

## The One-Sentence Summary

A processing module knows its inputs and outputs. The execution graph wires modules together by matching outputs to inputs. The runtime orchestrates files through the graph. The data store keeps every artifact immutable, checksummed, and catalogued with full provenance. StoreClient provides read-only access — typed filters or SQL — to everything the pipeline has produced.
