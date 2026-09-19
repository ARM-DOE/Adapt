# Adapt Architecture Strategy (2026-06)

Forward-looking architecture review and roadmap. The current-state design is documented in
[docs/design/](../README.md) (documents 00–07); this set evaluates that architecture against
the **Architecture Constitution** and proposes where the platform must go over the next
5–10 years.

## The Constitution (summary)

Adapt is a **scientific workflow platform**, not a radar framework. Radar, AI, remote
sensing, and tracking are plugins. Ten principles govern every recommendation:

1. **Core must be small** — contracts, pipelines, execution, provenance, registries, configuration, plugin discovery. Nothing else.
2. **Everything is a module** — the engine understands only `inputs → module → outputs`.
3. **Composition over inheritance.**
4. **Pipelines are first-class** — users consume workflows, not algorithms.
5. **Provenance is mandatory** — every artifact answers what/when/which version/which inputs/which config/which environment.
6. **Scientists write science** — `compute(inputs) -> outputs` and nothing more.
7. **Plugins over modifications** — adding capability never edits framework code.
8. **Stable contracts** — contracts evolve slowly; implementations evolve freely.
9. **Reproducibility before performance.**
10. **Simplicity wins** — one simple abstraction beats five clever ones.

Every recommendation in these documents is scored on: contributor friendliness,
extensibility, testability, reproducibility, operational complexity, and long-term
maintainability.

## Documents

| # | Document | One-line verdict |
|---|----------|------------------|
| 01 | [Architecture Vision](01_architecture_vision.md) | Domain-neutral kernel + domain distributions; do **not** build a generic workflow engine |
| 02 | [Extensibility Strategy](02_extensibility_strategy.md) | Registry is sound; replace YAML-listed import-time registration with entry-point discovery |
| 03 | [Pipeline Architecture](03_pipeline_architecture.md) | Keep the micro-DAG engine; kill phase integers; make per-scan processing a pure function |
| 04 | [Reproducibility Framework](04_reproducibility_framework.md) | Provenance schema exists but is unwired — make it mandatory and framework-injected |
| 05 | [Data Contract Architecture](05_data_contract_architecture.md) | Replace ad-hoc check functions with declarative per-key data specs; one canonical data model |
| 06 | [Model Registry Architecture](06_model_registry_architecture.md) | Models are versioned artifacts + ordinary modules; SQLite-first, no MLflow server in core |
| 07 | [Contributor Experience Review](07_contributor_experience.md) | **Critical question: currently NO** — a new module touches 4–6 files in 4 packages; target: one file |
| 08 | [Architecture Smell Analysis](08_architecture_smells.md) | 12 ranked findings; 2 critical (runtime knows module outputs by name; swallowed import failures) |
| 09 | [Production Readiness Assessment](09_production_readiness.md) | Edge/site deployment nearly ready; cloud/HPC blocked on filesystem-only persistence and packaging |
| 10 | [Open-Source Sustainability Plan](10_oss_sustainability.md) | Phased governance, API stability tiers, SemVer with explicit deprecation policy |

## Headline findings

1. **The skeleton is right.** Declared-IO modules, an auto-wired DAG, import-linter-enforced
   boundaries, architecture fitness tests, frozen validated config, and a single artifact
   repository are exactly the bones a 10-year platform needs. None of the recommendations
   here require a rewrite.
2. **The critical question fails today.** A radar scientist cannot contribute by writing
   `compute(inputs) -> outputs`. They must create a science class, a separate node wrapper,
   edit a central YAML list, understand phase integers and `required_history`, and know the
   config-slicing protocol. Document 07 shows the path to a one-file module.
3. **Provenance is a façade.** The catalog has `parent_ids` lineage columns, but the
   processor always writes `parent_ids=[]`; the environment, code version, and config
   content are never captured. This violates Principle 5 outright. Document 04 is the
   highest-leverage investment in the whole plan.
4. **The runtime violates Open/Closed.** `RadarProcessor._save_results` hardcodes the
   output keys of specific modules (`cell_stats`, `tracked_cells`, …). Every new core
   output means editing the runtime — the definition of Principle 7 failure. The fix
   (declarative persistence specs, already half-built as `OutputTableSpec`) is Document 08's
   top finding.
5. **Domain names leak into the kernel.** `nexrad_file`, `NexradPipeline`, hardcoded
   `"reflectivity"` in a contract while variable names are configurable elsewhere.
   Document 01 defines the boundary; Document 05 fixes the data model.

## Roadmap (sequenced by leverage, not effort)

| Phase | Theme | Key items |
|-------|-------|-----------|
| 1 (now) | Stop the bleeding | Delete dead `NexradPipeline`; make module import failures fatal; wire `parent_ids`; RunManifest |
| 2 | One-file modules | Merge module/node split; entry-point discovery; `adapt module new` scaffold; kill phase ints for declarative triggers |
| 3 | Contracts v1 | Canonical data model; per-key DataSpecs; CF-compliant outputs; contract version |
| 4 | Platform split | `adapt-core` / `adapt-radar` / `adapt-viz` packaging; fsspec persistence adapter; container images |
| 5 | Scale-out | Chunked batch CLI; model registry; multi-instrument distributions |
