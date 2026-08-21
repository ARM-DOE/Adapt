# Adapt Architecture Contract

This is the authoritative statement of Adapt's architecture. It is short on
purpose: every rule here is machine-enforced, and the enforcement point is
named next to the rule. If you change the architecture, change the enforcement
and this document in the same commit — `tests/test_architecture.py` fails if
this file, `.importlinter`, and the source tree disagree.

## Layer stack

A package may import only packages in layers **below** its own. Packages on
the same line (joined by `|`) are independent siblings and never import each
other. Enforced by `lint-imports` (`.importlinter`, contract "Adapt layer
stack", `exhaustive = true` — a new top-level package fails CI until it is
deliberately placed here **and** there).

```layers
cli
consumers | visualization
runtime
configuration
api | execution
modules | persistence
contracts | downloaders | utils
```

| Package | Role |
|---|---|
| `cli` | Outermost shell (`adapt <object> <action>`); may import anything, nothing imports it. |
| `consumers/` | Dashboard, target selection. Read **only** via `adapt.api.StoreClient`. |
| `visualization/` | Plotting. Core never imports it; matplotlib is confined here + consumers. |
| `runtime/` | Composition root: orchestrator, processor, postprocessor. The only place everything is wired together. |
| `configuration/` | Pydantic schemas, three-tier resolution, frozen `InternalConfig`, `defaults.yaml` module list. |
| `api/` | `StoreClient` — the read-only public facade over the store. |
| `execution/` | Graph builder/executor + `nodes/`: the one sanctioned wiring layer over modules. |
| `modules/` | Scientific modules (ingest, detection, projection, analysis, tracking, …). Deterministic, no store I/O. |
| `persistence/` | SQLite catalogs + NetCDF objects. No science, no orchestration. |
| `contracts/` | Frozen dataclasses, Protocols, and `check_*` validators. Zero adapt imports. |
| `downloaders/` | All boto3/S3 calls. Zero adapt imports. |
| `utils/` | Shared pure functions. Zero adapt imports. |

Optional plug-in modules are loaded via the `extensions:` list of dotted import
paths in the run configuration — there is no in-tree extensions package.

## The eight rules

1. **Scientific modules** (`modules/*`) import only `contracts`, `utils`
   (plus `downloaders` for acquisition). They never touch the store, config,
   runtime, or each other. Config arrives by injection as a frozen
   `<name>_config` context entry.
   *(lint-imports layers; `tests/test_architecture.py` module-independence tests)*
2. **Modules are deterministic**: no wall clock, no global RNG — identical
   input + config ⇒ identical output.
   *(AST bans + run-twice determinism tests)*
3. **Inter-module data flows through the context dict** via declared
   `inputs`/`outputs`. Every output gets a `check_*` validator in `contracts/`;
   the uncontracted allowlist only shrinks.
   *(context-key coherence tests + `_UNCONTRACTED_OUTPUTS` ratchet)*
4. **Adding a module** = module package + `execution/nodes/` wrapper + one
   line in `configuration/defaults.yaml` + contract validators. Zero edits
   anywhere else. If you are editing `runtime/` to add a module, stop — the
   design is wrong.
   *(registration tests; new module auto-discovered by all fitness tests)*
5. **Consumers read only through `adapt.api.StoreClient`.** No
   sqlite/duckdb, no NetCDF opens, no globs, no store paths.
   *(lint-imports forbidden contract + consumer AST fitness test)*
6. **Every heavy third-party dependency has exactly one home** —
   `_DEP_HOMES` in `tests/test_architecture.py` is the authoritative list.
   A new dependency = `pyproject.toml` entry + `_DEP_HOMES` entry in the
   same change.
   *(dependency-home test + declared-dependencies test)*
7. **Public API = `adapt.api.__all__`** plus the documented consumer entry
   points (`adapt.consumers.live.main`, target-selection exports). Everything
   else is internal and may change without notice.
8. **Fail loudly.** No fallbacks, no bare `except`, no silent defaults.

## Product tables: core + extras

Science outputs vary by data source (NEXRAD and ARM radars ship different
variables), so product-table schemas are frozen at the first write of each
run — per run, per source, the column set is stable. Two rules keep that
flexible without being fragile:

- **Core identity columns** (`run_id`, `scan_id`, `scan_time`,
  `scan_time_unix`, `valid_time`, `cell_uid`, `cell_label`) have contracted
  types, fixed by name in `persistence/products.py` (`_CORE_COLUMN_TYPES`).
  The writer stamps scan identity; modules never pass it. Readers may
  hardcode these names.
- **Everything else is an extra**: source-dependent, frozen by a
  deterministic rule (numeric → REAL, bool → INTEGER, else TEXT) so the
  schema is a function of the column *names* — never of the values in
  whichever scan happened to arrive first. Readers discover extras through
  the `table_schemas` snapshots, never by hardcoding.

Static bookkeeping tables (scans, runs, artifacts, history, tracking core)
are defined in `configuration/schemas/*.sql` — the single DDL home. A
shrink-only ratchet in `tests/test_architecture.py` pins the remaining
legacy exception (`track_store.py`) and rejects any new `CREATE TABLE`
outside it.

Variable names are canonicalized once at ingest via `reader.field_map` /
`reader.fields`; downstream code never maps names. The applied mapping is
recorded in each grid's `source_fields_json` attribute as provenance.

## Decisions not to relitigate

See `docs/design/` for the full decision records.

- `scan_id` = sha256[:16] of the raw file bytes is the join key everywhere;
  `scan_time` is single-format ordering/display metadata
  (`adapt.utils.time.to_scan_iso` is the only serializer).
- `StoreClient` is read-only with typed filters — consumers never write.
- Module registration lives in `defaults.yaml`, not entry points.
- Contracts are validators and frozen dataclasses, not ABCs.
- The store is SQLite catalogs + NetCDF objects; conversion happens once at
  the boundary.

## Evidence required with any change

All green in CI before a change is accepted:

```
ruff check src tests
lint-imports
pytest            # integration tests are opt-in: pytest -m integration
mypy src/adapt
```

New behavior ships with a test. A new invariant ships with a fitness function.
An allowlist never grows silently — extend the mechanism instead.
