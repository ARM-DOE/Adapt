# Document 2 — Extensibility Strategy

Goal: a contributor adds an algorithm, retrieval, classifier, tracker, feature
extractor, ML model, visualization, exporter, or data reader **without modifying any
existing framework code** (Principle 7).

## Current state, assessed

What exists is closer to the right answer than most projects at this stage:

| Mechanism | Where | Verdict |
|-----------|-------|---------|
| Single module interface (`BaseModule`: name, inputs, outputs, contracts, `run`) | `src/adapt/modules/base.py` | **Keep.** One flat ABC, no hierarchy — already satisfies Principles 2 & 3 |
| Name-keyed registry with duplicate detection | `src/adapt/execution/module_registry.py` | **Keep.** 120 lines, boring, correct |
| Auto-DAG from declared IO | `src/adapt/execution/graph/builder.py` | **Keep.** This is the platform's best idea |
| Per-module Pydantic `config_class` + dynamic config.yaml generation | `BaseModule.default_params()` | **Keep and make universal** |
| Source plugins (`source_registry`) | `src/adapt/runtime/sources.py` | **Keep** — proof that "everything is a module" can extend to ingress |
| Registration by import side-effect, driven by a YAML list of dotted paths | `pipeline_builder._ensure_modules_registered` + `configuration/defaults.yaml` | **Replace** — this is the extensibility bottleneck |
| Module/node split (`modules/x/module.py` + `execution/nodes/x.py`) | both packages | **Merge** — doubles contributor surface for no architectural gain (see Doc 07) |

The failures against Principle 7, concretely:

1. **Adding a core module edits framework files.** A new in-tree module requires adding
   its dotted path to `defaults.yaml`, and (for typed config) a new section in
   `configuration/schemas/internal.py` plus resolution code. That is "modification,"
   not "plugin."
2. **Import failures are swallowed.** `_ensure_modules_registered` catches any exception
   from a core module import, logs, and continues — the pipeline then assembles
   *without that module* and fails later with a misleading missing-input error. There
   is also a hardcoded fallback module list if the YAML is unreadable. Both directly
   violate the project's own "no fallbacks" constitution. (Doc 08, S2.)
3. **Third-party modules require config plumbing.** `config.extensions` (dotted import
   paths in user config) works, but the extension's package must be importable and the
   user must know its internal module path. Discovery should come from packaging
   metadata, not user-maintained path lists.

## Discovery mechanism — options evaluated

### Option A: status quo (YAML-listed import side-effects)
Registration happens when a listed module file is imported. Contributor must edit a
framework YAML (core) or maintain dotted paths in user config (extension).
- Contributor friendliness **Low** · Extensibility **Low** · Testability Med ·
  Reproducibility Med (the module list is config, good) · Op complexity Low ·
  Maintainability **Low** (every module addition is a core diff).

### Option B: Python entry points (recommended)
Packages declare modules in their own `pyproject.toml`:

```toml
[project.entry-points."adapt.modules"]
hail_detect = "adapt_hail.module:HailDetectModule"
```

The kernel discovers with `importlib.metadata.entry_points(group="adapt.modules")`.
Installing a package makes its modules available; uninstalling removes them. Core
modules use the same mechanism in `arm-adapt`'s own `pyproject.toml` — the kernel has
no privileged module list at all, and `defaults.yaml`'s `pipeline.modules` section is
deleted.
- Contributor friendliness **High** (declare in your own package, done) ·
  Extensibility **High** · Testability High (register classes directly in tests, as
  today) · Reproducibility **High** — *provided* the run manifest records every
  discovered module's name+version+package (Doc 04), because "what code was available"
  now depends on the environment, which the manifest must pin ·
  Op complexity Low (stdlib only) · Maintainability **High**.

### Option C: pluggy (pytest's plugin engine)
Hook-based: plugins implement named hooks; the framework calls hook chains.
Powerful for *behavioral* extension (modify how the framework itself works), wrong
shape for Adapt: modules are *data-flow* nodes, not hook implementations. Adopting
pluggy would invite plugins that mutate framework behavior — the opposite of a stable
kernel. Also a new dependency and a new mental model for scientists.
- Contributor friendliness Low (hookspec concept) · Extensibility High but in the wrong
  dimension · Maintainability Med. **Rejected.**

### Option D: `__init_subclass__` auto-registration
Subclassing `BaseModule` registers automatically. Less explicit, magic-feeling,
registration still requires the defining file to be imported — so it solves nothing
about discovery and removes the explicit `registry.register(...)` line that makes
behavior obvious. **Rejected** (Principle 10: explicit beats clever).

**Recommendation: B.** Keep `ModuleRegistry` exactly as is; feed it from entry points
instead of YAML-listed imports. The registry stays the single runtime authority
(testable, clearable); entry points are only the discovery transport. One loading rule:
**any module that is *selected for execution* and fails to import raises immediately**;
modules that are discovered but not selected may fail to import with a warning naming
the package (so one broken extension can't brick unrelated workflows, but you can never
silently run without a module you asked for).

## The module taxonomy — everything is a module, mapped

Principle 2 demands one engine concept. Concretely:

| Capability | Today | Target |
|------------|-------|--------|
| Data readers / sources | Separate `SourceRegistry` + `ScanSource` thread interface | Module with `outputs=["scan_ref"]`, no inputs; the *scheduling* of a source (poll loop, queue) is runtime policy, not a different interface |
| Algorithms (detection, projection…) | `BaseModule` ✓ | unchanged |
| Trackers | `BaseModule` ✓ (stateful via `required_history`) | unchanged, with declarative state (Doc 03) |
| Retrievals / feature extractors | `BaseModule` (e.g. `cell_volume_stats`) ✓ | unchanged |
| ML models | none | module + model registry reference (Doc 06) |
| Validators | hardcoded contract callables | per-key DataSpecs (Doc 05); custom validators are modules with no outputs |
| Exporters | none (processor hardcodes persistence) | module with declared `output_table`/artifact spec — generalize the existing `OutputTableSpec` mechanism |
| Visualization | in-core `visualization/` + Tkinter dashboard | Ring 2 consumer packages reading via `adapt.api`; never DAG nodes |

Two real exceptions, named honestly rather than forced into the abstraction:
**sources** need a lifecycle (start/stop/poll) the engine drives, and **consumers**
run outside the pipeline entirely. Pretending these are plain `compute()` modules
would be a lie; keeping them as two small, separate, stable interfaces is simpler
(Principle 10) than one bloated universal one.

## Dependency management

- Extensions declare their own dependencies in their own packages — the kernel never
  gains a dependency because a plugin needs one (today `opencv-python`, `arm_pyart`,
  `nexradaws` are kernel deps; after the Ring split they move to `adapt-radar`).
- Missing optional dependency = immediate `ImportError` with the install hint
  (the existing `lma`/pyxlma pattern, made the documented norm). No degraded modes.
- The kernel pins **nothing** scientific; domain distributions pin their own stacks;
  operational deployments use lock files (Doc 04, environment capture; Doc 09).

## Version compatibility

The thing that actually breaks ecosystems is silent contract drift. Strategy:

1. `adapt.contracts` carries `CONTRACT_VERSION` (SemVer, independent of package version).
2. Every module declares `requires_contracts = ">=1,<2"` (a ClassVar with a default,
   so most contributors never think about it).
3. The registry checks compatibility at selection time and fails with a message naming
   the module, its package, and both versions. No warnings-and-continue.
4. Context keys and table schemas are versioned with the contracts (Doc 05); the
   catalog records the contract version per run (Doc 04), so old data remains
   interpretable forever.

This is deliberately coarse — one version for the whole contract surface, not per-key
versioning. Per-key versioning is the "5 clever abstractions" trap; revisit only if
the single version demonstrably forces lockstep upgrades across unrelated domains.

## Migration path (no big bang)

1. Add entry-point loading alongside the YAML path; core modules declare entry points.
2. Switch the processor to entry-point discovery; delete `pipeline.modules` from
   `defaults.yaml` and the hardcoded fallback list; make selected-module import
   failures fatal. (~1 week, mostly tests.)
3. Merge `execution/nodes/*` into `modules/*` (Doc 07) so a module is one package.
4. Publish a cookiecutter extension template (`adapt-extension-template`) with entry
   point, config class, tests, and CI preconfigured.
5. When the second domain distribution appears, extract `adapt-core`.

## Scoring the recommended strategy

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | Add a package, declare an entry point; zero framework edits |
| Extensibility | High | In-tree and third-party modules use the identical mechanism |
| Testability | High | Registry unchanged; tests inject classes directly, no packaging needed |
| Reproducibility | High* | *Conditional on run-manifest recording of discovered modules (Doc 04) |
| Operational complexity | Low | stdlib `importlib.metadata`; no plugin server, no new deps |
| Long-term maintainability | High | Kernel loses its module list; module churn stops touching core |
