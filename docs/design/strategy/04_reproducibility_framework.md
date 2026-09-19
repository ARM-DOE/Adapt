# Document 4 — Scientific Reproducibility Framework

Principle 5: every artifact must answer **what produced this, when, which version,
which inputs, which configuration, which environment** — with zero user effort.
Principle 9: when reproducibility and performance conflict, reproducibility wins.

## Current state: a provenance façade

The schema is ahead of the implementation. What exists:

| Capability | Status |
|------------|--------|
| Per-run identity (`run_id`) on every artifact and table row | ✓ wired |
| Artifact catalog (SQLite) with `parent_ids` lineage column | Schema ✓ — **never populated**: `processor.py` passes `parent_ids=[]` on every write |
| Configuration provenance | `runs` table stores `config_path` — a *path*, which can be edited or deleted after the run; the resolved config content is not snapshotted or hashed |
| Software provenance | ✗ nothing: no package version, no git SHA, no environment capture anywhere in the catalog |
| Pipeline provenance | Partial: log lines list enabled modules; nothing machine-readable records which modules ran, in what versions, with what triggers skipped |
| Data provenance (inputs) | Raw file path stored in NetCDF `attrs["source"]`; no checksum, no AWS object version |
| Time canonicalization | ✓ excellent — single `to_scan_iso` source enforced by a fitness test (`tests/test_architecture.py`) |
| Run finalization status | ✓ `finalize_run("completed"/"cancelled")` |

So today an artifact answers "which run and which scan" but not what/version/inputs/
config/environment. Principle 5 fails. The good news: every fix below lands in Ring 0
(persistence + runtime) and requires **zero work from module authors** — provenance is
framework-injected, never module-written (Principle 6).

## Design: three layers of provenance

### Layer 1 — RunManifest (written once, at run start, before any science)

An immutable JSON artifact, registered in the catalog like any other artifact:

```json
{
  "run_id": "abc12345",
  "created_at": "2026-06-10T14:02:11Z",
  "adapt_version": "0.9.2",
  "contract_version": "1.2.0",
  "code": {"git_sha": "86da841", "dirty": false},
  "environment": {
    "python": "3.13.1", "platform": "Linux-5.14-x86_64",
    "packages": {"numpy": "2.1.0", "arm_pyart": "2.0.1", "...": "..."}
  },
  "pipeline": {"name": "nexrad_tracking", "version": 2, "sha256": "..."},
  "modules": [
    {"name": "detection", "package": "arm-adapt", "version": "0.9.2"},
    {"name": "hail_detect", "package": "adapt-hail", "version": "1.3.0"}
  ],
  "config": {"sha256": "...", "snapshot": { "...resolved InternalConfig..." }},
  "seeds": {"global": 42}
}
```

Notes:
- **Config snapshot is the resolved `InternalConfig`** (it is already frozen and
  serializable) — not the user's YAML, which is one input to resolution. Store both
  the snapshot and its SHA-256; the hash becomes the cache/comparison key (Doc 03).
- **Package capture** is `importlib.metadata.distributions()` — cheap, stdlib, no pip
  subprocess. `git_sha` is captured when running from a checkout; absent (not faked)
  for installed wheels, where `adapt_version` suffices.
- **Module list comes from the registry at selection time** — this is the condition
  that makes entry-point discovery (Doc 02) reproducible.
- Writing the manifest is **mandatory and fail-loud**: if it cannot be written, the
  run does not start. Provenance cannot be optional.

### Layer 2 — Artifact lineage (per write)

1. **Make lineage required.** (Done in the store: `ObjectStore.commit` takes parents and every non-raw artifact records lineage to its raw volume.) Change the writer /
   `write_parquet` signatures so the caller must pass parents (possibly explicitly
   `parents=[]` for true roots like the raw download). Silent-empty defaults are how
   the current façade happened. Fail loud (project constitution).
2. **The engine computes parents automatically.** Because every node declares inputs,
   the `ExecutionPlan` (Doc 03) knows which upstream artifacts fed each output:
   raw file → gridded NetCDF → analysis NetCDF → tables. Module authors never see any
   of this — the persistence specs they declare (Doc 08, S1) carry lineage for free.
3. **Checksum inputs at the boundary.** SHA-256 of each raw file at ingest, stored on
   the root artifact. Cost: milliseconds per scan against multi-second processing —
   acceptable under Principle 9.
4. **Every module execution gets a record**: `(run_id, scan_time, module, version,
   status=completed|skipped:<trigger>|failed, duration, error)` in a `module_executions`
   table. This is the "pipeline provenance" that currently exists only as log lines,
   and it makes *skipped* (e.g., time-gap exceeded) a recorded scientific fact rather
   than an absence.

### Layer 3 — Self-describing artifacts (embedded metadata)

Catalogs get lost; files travel. Every NetCDF artifact carries, in `attrs`:
`run_id`, `artifact_id`, `parent_ids`, `adapt_version`, `contract_version`,
config hash, and a CF-style `history` line
(`2026-06-10T14:02Z: adapt 0.9.2 detection(threshold=35.0) run=abc12345`).
Every table already carries `run_id`/`scan_time` per row — sufficient, given the
catalog. Framework-injected in the writer, invisible to modules.

## Metadata standards — recommendations

| Standard | Use | Recommendation |
|----------|-----|----------------|
| CF Conventions (`history`, `source`, `references`) | NetCDF global attrs | Adopt — reviewers and ARM tooling expect it (also Doc 05) |
| ACDD (`date_created`, `creator`, `id`) | NetCDF discovery attrs | Adopt the small useful subset; don't chase full compliance |
| W3C PROV | Interchange | **Do not** model internally in PROV (Entity/Activity/Agent triples are the 5-clever-abstractions trap). Keep the simple internal model (run → module execution → artifact) and provide a PROV-JSON *exporter* for interop |
| RO-Crate | Packaging a run for publication | Future exporter (`adapt run export`), same internal model |

## The user-facing payoff

```bash
adapt provenance <artifact_id>     # walk lineage up to raw inputs; print versions, config hash
adapt run diff <run_a> <run_b>     # diff manifests: config keys, package versions, module versions
adapt run export <run_id>          # bundle manifest + catalog slice (+ optional artifacts) for publication
```

`run diff` is the sleeper feature: "why do these two reprocessings differ?" is the
question scientists actually ask, and the manifest answers it mechanically.

## Determinism policy (what reproducibility requires beyond provenance)

1. **All randomness is seeded from config.** Any module using RNG draws from a seed in
   its config class; the seed lands in the manifest. CI runs a determinism test: same
   inputs twice ⇒ identical table content and NetCDF data variables.
2. **No wall-clock in science outputs.** `datetime.now()` is allowed in bookkeeping
   columns (`updated_at`) but never in scientific values. (The current
   `scan_time = datetime.now(UTC)` fallback in `_save_analysis_netcdf` when scan_time
   is None violates this — it should raise. Doc 08.)
3. **Bit-reproducibility is scoped honestly.** Across the *same* environment: required,
   tested. Across BLAS/OS/architecture changes: not promised — instead, the manifest
   records the environment and operational deployments pin lock files (conda-lock /
   uv lock per release), which is the scientifically defensible position.
4. **Parallelism never reorders science** (Doc 03): deterministic chunk boundaries,
   stable sorts at stitch seams, no reduction-order dependence.

## What this deliberately does not include

- No content-addressed storage overhaul (artifact IDs + hashes suffice; CAS is a
  performance optimization with real migration cost — revisit with the cache, Doc 03).
- No automatic data versioning of *inputs* (DVC-style). Raw NEXRAD archives are
  immutable and externally curated; a checksum is enough.
- No blockchain, no signed attestations — until a funder requires them.

## Migration order

1. RunManifest + mandatory `parents=` parameter + module_executions table. (1–2 weeks;
   no module changes)
2. Engine-computed lineage from declared IO (lands with the ExecutionPlan, Doc 03).
3. Embedded NetCDF provenance attrs + `adapt provenance` CLI.
4. Determinism CI test + seed policy; `run diff`; exporters last.

## Scoring

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | Module authors do nothing; provenance is injected |
| Extensibility | High | Plugins inherit full provenance with zero code |
| Testability | High | Manifest and lineage are plain data; determinism test is mechanical |
| Reproducibility | High | This *is* the reproducibility investment; answers all six questions |
| Operational complexity | Low | JSON + existing SQLite catalog; no services |
| Long-term maintainability | High | One simple internal model; standards handled as exporters |
