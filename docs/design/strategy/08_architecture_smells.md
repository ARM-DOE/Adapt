# Document 8 — Architecture Smell Analysis

A code-level audit of the current architecture against its own stated rules
(CLAUDE.md, `.importlinter`) and the constitution. Every finding cites the code.
Findings are ranked by severity = (damage if unfixed at 100 modules) × (cost growth
over time).

## What is *right* (so it doesn't get refactored away)

- **Boundary enforcement is real, not aspirational**: seven import-linter contracts +
  auto-discovering architecture tests (`tests/test_architecture.py`) that need no
  edits for new modules. Rare and valuable.
- **One flat module ABC** (`BaseModule`), no inheritance towers — Principle 3 already
  satisfied.
- **Frozen, validated runtime config** (`InternalConfig`, `frozen=True`,
  `extra="forbid"`) with the explicit no-`.get()`-in-runtime rule.
- **Single-source scan-time serialization** guarded by a fitness test — exemplary
  DRY-by-enforcement.
- **Error taxonomy** (`ContractViolation` vs `ValueError` vs recoverable) is clean,
  and contract violations correctly halt the pipeline.
- **Repository pattern with atomic writes**; consumers read via `adapt.api` instead
  of touching files.

The smells below are mostly cases where the codebase violates *its own* principles —
which is the easiest kind of debt to sell fixing.

---

## S1 — CRITICAL: The runtime knows module outputs by name

**Where:** `runtime/processor.py:_save_results` (and `_build_enrich_context`,
`_should_run_enrichment`, `_attach_cell_uid_lut`).

**What:** The persistence step hardcodes the keys `grid_nc_path`, `cell_stats`,
`cell_adjacency`, `tracked_cells`, `cell_events`, `projected_ds`, branches on their
combinations, and even raises if `tracked_cells` arrives without `cell_adjacency`.
The runtime — supposedly module-agnostic orchestration — contains a hand-written
routing table for the science.

**Why it matters:** Every new core output requires editing the runtime: the canonical
Principle 7 failure. At 8 modules it's an annoyance; at 100 it's a god-function and a
merge-conflict magnet. It also makes the engine untestable against synthetic modules
without replicating NEXRAD-specific keys.

**Fix:** The codebase already contains the right pattern — `output_table:
OutputTableSpec` lets *extension* modules declare persistence and the framework
routes it generically (`ModuleOutputWriter`). Generalize it to everything: modules
declare artifact specs (table | netcdf | none) per output key; the engine persists
declared outputs mechanically; cross-output invariants like "tracked cells need
adjacency" become input declarations of the consuming persistence spec
(`TrackStore` write becomes a spec consuming three keys). Hidden coupling becomes
declared dependency.

## S2 — CRITICAL: Swallowed core-module import failures + hardcoded fallback list

**Where:** `execution/pipeline_builder.py:_ensure_modules_registered` — core module
import failure is `logger.error(...)` then *continue*; unreadable `defaults.yaml`
falls back to a hardcoded five-module list.

**What/why:** Both branches directly violate the repo's own hard constraint ("No
fallbacks. Missing dependency, config, or file → raise immediately"). A failed
detection import yields a pipeline silently assembled *without detection*, which then
fails — if you're lucky — with a "missing input `segmented_ds`" error pointing
nowhere near the cause. Silent partial assembly in a scientific pipeline is a
correctness hazard, not a robustness feature. Extensions, notably, already get the
correct fail-loud treatment three lines later — the inconsistency proves the fix is
known.

**Fix:** Selected-module import failure ⇒ raise with package+module+original
traceback. Delete the fallback list. (Subsumed by entry-point discovery, Doc 02, but
worth fixing *now* — it's a 5-line change.)

## S3 — HIGH: Dead second assembly path (`NexradPipeline`)

**Where:** `execution/pipeline_builder.py:132–202`. Referenced by zero call sites in
`src/` and `tests/` (verified by grep).

**What/why:** A full alternative pipeline-assembly class that ignores
`pipeline_phase`, `required_history` grouping, and module enable/disable resolution —
i.e., it would assemble a *different, wrong* pipeline if anyone used it. Violates the
repo's "No artifacts / if it is not called, it does not exist" rule, and it's the
exact kind of doc-bait that misleads new contributors (its docstring presents itself
as "how the controller builds the pipeline").

**Fix:** Delete the class; move `_ensure_modules_registered` and
`resolve_enabled_modules` (both genuinely used) to honestly-named homes.

## S4 — HIGH: The context dict is an unowned, undocumented interface

**Where:** Everywhere — `GraphExecutor.run(context)`, every module's `run(context)`.

**What:** The de-facto interface of the entire platform is a `dict[str, Any]` whose
keys (`grid_ds_2d`, `segmented_ds`, `scan_history`,
`detection_config`, …) have no authoritative definition, no type, no ownership, and
mixed *kinds*: science data, module configs, infrastructure handles (`repository` —
which also hands modules a loophole around the "modules never touch persistence"
boundary), and orchestration state all share one namespace.

**Why it matters:** String-keyed implicit coupling is the thing that stops scaling at
"hundreds of modules": key collisions, archaeology-driven development (Doc 07), and
no tooling can answer "who produces/consumes X?".

**Fix:** Per-key DataSpecs as the key registry (Doc 05); infrastructure handles move
out of the science namespace (injected as declared capabilities, not dict entries);
`compute(**inputs)` binding hides the dict entirely (Doc 07).

## S5 — HIGH: Orchestration leaks into module declarations (`pipeline_phase`, `required_history` grouping)

**Where:** `modules/base.py` (`pipeline_phase`, `POSTPROCESS_PHASE=4`,
`required_history`); `runtime/processor.py` (`!= 3`/`== 3` partitions, per-history
executor groups, manual `_scan_history`).

**What/why:** The DAG was supposed to be derived from declared IO; in reality magic
integers and history-group partitioning decide the macro-topology, and the processor
sequences the groups by hand. Scientists choosing a phase number is Principle 6
inverted. Two executors with hand-merged contexts also mean edge contracts don't
guard the seam between groups.

**Fix:** Declarative `trigger`/`state: windowed(n)` compiled into one ExecutionPlan
(Doc 03). Phases 0–2 are already redundant with IO ordering today.

## S6 — MEDIUM: Contract/config contradiction on variable names

**Where:** `contracts/grid.py:45` hardcodes `"reflectivity"`;
`InternalConfig.global_.var_names` makes the same name user-configurable; modules
dutifully thread `reflectivity_var`/`labels_var` through their configs.

**What/why:** A user who exercises the configurability breaks the contracts; the
flexibility is therefore fake, but everyone pays its plumbing cost. Half-abstractions
are worse than either extreme.

**Fix:** Canonical fixed names; delete `var_names`/`coord_names` (Doc 05, Decision 1).

## S7 — MEDIUM: Two-tier config system; central schema edits for core modules

**Where:** `configuration/schemas/internal.py` (hand-written `segmenter`, `projector`,
`analyzer`, `tracker`… sections) vs `module_params: dict[str, dict]` for extensions;
plus three per-module mechanisms (`config_class`, `build_config()` slicing,
`injected_global_fields`).

**Why:** Core modules get typed config by editing a central schema (Principle 7
violation); extensions get an untyped dict (worse validation for exactly the code
that needs it most). Three mechanisms where one suffices.

**Fix:** Registry-assembled config: every module's `config_class` is composed into
the resolved config dynamically; globals become declared, resolver-filled fields.
`InternalConfig` keeps only kernel concerns (mode, dirs, source, logging, run_id).

## S8 — MEDIUM: Provenance schema unwired

**Where:** `parent_ids=[]` at `runtime/processor.py:455`; `runs.config_path` stores a
path, not content; no environment/code capture anywhere.

**Why/fix:** Principle 5 fails outright. Full treatment in Doc 04 (RunManifest,
mandatory parents, module_executions). Listed here because it is *debt*, not just a
missing feature: the longer runs accumulate without lineage, the more stored science
is permanently unanswerable.

## S9 — MEDIUM: Ring violations in packaging — visualization and heavy deps in core

**Where:** `pyproject.toml` mandatory deps include `matplotlib`, `opencv-python`,
`arm_pyart`, `nexradaws`, `duckdb`, `pyarrow`; `adapt/visualization/` and the Tkinter
`consumers/live` dashboard live in the core package.

**Why:** CLAUDE.md itself says plotting lives in a separate optional package.
Practical costs: server/edge installs drag GUI stacks; import weight; the kernel
can't be reused by a non-radar domain without Py-ART. (Import-linter keeps *code*
boundaries clean — packaging boundaries are the unenforced half.)

**Fix:** Extras first (`pip install arm-adapt[viz,nexrad]`) — cheap, immediate; then
the Ring split (`adapt-core`/`adapt-radar`/`adapt-viz`) per Doc 01.

## S10 — LOW: Engine implementation limits

**Where:** `execution/graph/executor.py` — O(n²) ready-polling loop, sequential only;
`GraphBuilder` silently treats an input nobody produces as "comes from initial
context" (a typo'd input key becomes a missing-key error at run time, not build
time).

**Fix:** Kahn's algorithm + build-time validation that every input is either produced
by a module or declared as an engine-provided key. ~50 lines. Not urgent at 8 nodes;
required before "hundreds."

## S11 — LOW: Naming and constant drift

**Where:** `ProductType` (`GRIDDED_NC = "gridded3d"`, `ANALYSIS_NC = "segmentation2d"`,
two `DEPRECATED` constants with no removal ticket — violating the repo's own
"flag obsolete code + open ticket" rule); `nexrad_file` as the universal context key;
`RadarProcessor.get_results/save_results/close_database` kept as no-op stubs.

**Fix:** One rename pass + delete the stubs; rename `nexrad_file` → `scan_ref` when
the source interface generalizes (Doc 02).

## S12 — LOW: Wall-clock fallback in scientific metadata

**Where:** `processor._save_analysis_netcdf`: `if scan_time is None: scan_time =
datetime.now(UTC)`.

**Why:** A scan artifact stamped with processing time instead of observation time is
silently corrupted provenance — and it's a *fallback*, forbidden by the constitution.
**Fix:** raise.

---

## Summary matrix

| # | Finding | Severity | Principle violated | Effort | Fix doc |
|---|---------|----------|--------------------|--------|---------|
| S1 | Runtime hardcodes module output routing | Critical | 7 (plugins over modifications) | M | 03, 08 |
| S2 | Swallowed imports + fallback module list | Critical | Repo's "no fallbacks" | XS | 02 |
| S3 | Dead `NexradPipeline` | High | Repo's "no artifacts" | XS | 03 |
| S4 | Unowned context-dict interface | High | 8 (stable contracts) | L | 05, 07 |
| S5 | Phase ints / history grouping | High | 6 (scientists write science) | L | 03 |
| S6 | Contract↔config name contradiction | Medium | 8, 10 | S | 05 |
| S7 | Two-tier config, central schema edits | Medium | 7 | M | 07 |
| S8 | Unwired provenance | Medium (compounding) | 5 (provenance mandatory) | M | 04 |
| S9 | Viz + heavy deps in core package | Medium | 1 (small core) | S→L | 01 |
| S10 | Executor O(n²), no build-time input check | Low | — | S | 03 |
| S11 | Naming/constant drift, no-op stubs | Low | hygiene | XS | — |
| S12 | Wall-clock fallback in metadata | Low | 5, no-fallbacks | XS | 04 |

**Over-engineering verdict:** remarkably little — the module/node split (Doc 07) and
the fake name-configurability (S6) are the only abstractions that cost more than they
return. **Under-engineering** is the dominant pattern: provenance, specs, and
persistence routing all have the right schema-shaped stubs that were never made
load-bearing. That is the cheapest possible position to fix from.
