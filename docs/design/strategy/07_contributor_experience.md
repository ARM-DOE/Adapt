# Document 7 — Contributor Experience Review

The constitution's critical question:

> A radar scientist wants to contribute a new algorithm. Can they do so by
> implementing `compute(inputs) -> outputs` without understanding the entire
> framework?

## Verdict: **No — not today.** Here is the walkthrough that proves it.

Persona: a radar scientist with solid Python/xarray skills wants to add a hail
detection algorithm that consumes the 3-D grid and per-cell stats and emits a per-cell
hail table. Following `docs/module-developer-guide.md` (which is honest about this —
it has a section literally titled "Complete change list for a new module"), they must:

| Step | File(s) touched | Framework knowledge required |
|------|-----------------|------------------------------|
| 1. Science class | `modules/hail/module.py`, `modules/hail/config.py` | Little — this is the part they came to write |
| 2. Node wrapper | `execution/nodes/hail.py` | `BaseModule` ClassVars; the **context-dict protocol and its key names** (`grid_ds_3d`? `cell_stats`? — discoverable only by reading other modules); lazy-instantiation idiom; `registry.register()` at import time |
| 3. Pick magic numbers | same file | `pipeline_phase` 0–4 semantics (0 vs 3 vs `POSTPROCESS_PHASE=4`) and `required_history` — **runtime orchestration concepts** with no business in a science contribution |
| 4. Config plumbing | same file (+ maybe `configuration/schemas/internal.py`) | Three overlapping mechanisms: `config_class` defaults, `build_config()` slicing of `InternalConfig`, `injected_global_fields`; plus the core-vs-extension fork (typed `InternalConfig` section vs untyped `module_params` dict) |
| 5. Contracts | `contracts/hail.py` (+ register check fns on the node) | `require()`/`ContractViolation` idiom; which existing checks to attach to inputs |
| 6. Persistence | `output_table = OutputTableSpec(...)` | Spec semantics, the scan-time canonicalization rule, core-vs-extension table guard |
| 7. Registration | `configuration/defaults.yaml` (core) or `extensions:` config list | The YAML list / dotted-path mechanism — **a framework file edit** (Principle 7 violation) |
| 8. Tests | `tests/...` (3+ locations by convention) | Synthetic-fixture conventions; which of unit/runtime/validation dirs |

That is **4–6 files across four packages** (`modules/`, `execution/`, `contracts/`,
`configuration/`), and — the deeper problem — concepts 2–4 are *orchestration*
knowledge, exactly what Principle 6 says scientists must not need. The existence of a
306-line developer guide, a 27 KB `MODULE_EXTENSION_GUIDE.md`, and an in-repo AI
scaffolding skill (`.claude/create-adapt-module`) is the system telling you the same
thing three ways: the manual path is too complex, so tooling grew around it instead of
the path being shortened. Scaffolding that *generates* boilerplate is a mitigation;
boilerplate that doesn't exist is the fix.

Credit where due: the floor is far better than most scientific frameworks. Declared
IO means no orchestration *code* is written; the auto-DAG means no pipeline file is
edited for wiring; architecture tests catch layering mistakes mechanically with clear
messages. The gap is concentrated and fixable.

## The architectural obstacles, named

1. **The module/node split.** `modules/x/module.py` (science) + `execution/nodes/x.py`
   (BaseModule wrapper) doubles the surface. Its stated purpose — keeping science
   import-clean of framework — is already guaranteed by a *cheaper* mechanism: the
   import-linter contracts and `tests/test_architecture.py`. `BaseModule` itself
   imports nothing but stdlib (`abc`, `typing`), so a science package subclassing it
   gains no forbidden dependency. The split is belt-and-suspenders paid for by every
   contributor on every module. **Merge them**: the module package contains one class
   that *is* the node; a separate pure-science class within the package stays an
   optional internal pattern for complex modules, not a requirement.
2. **Magic orchestration ints** (`pipeline_phase`, `required_history`) — replaced by
   declarative `trigger`/`state` with safe defaults (Doc 03), so a default module
   declares neither.
3. **Central registration edits** — replaced by entry points (Doc 02).
4. **Config trifurcation** — one mechanism: every module owns a `config_class`; the
   resolved config is assembled *from the registry* (so `InternalConfig` no longer
   hardcodes `segmenter`/`projector`/... sections); `build_config` slicing and
   `injected_global_fields` collapse into "your config class, validated, handed to
   you." Globals a module needs (e.g. `z_level`) become declared fields the resolver
   fills.
5. **Undocumented context keys** — fixed by per-key DataSpecs (Doc 05), which give
   the contributor a *catalog* of available inputs with schemas, instead of
   read-the-other-modules archaeology.

## The target: one file, one concept

What the hail scientist writes after Docs 02/03/05 land — in their own package or in
`adapt-radar`, identically:

```python
# adapt_hail/module.py — the ENTIRE contribution besides tests + pyproject.toml
from adapt.sdk import Module, Field

class HailDetect(Module):
    """Per-cell hail likelihood from 3-D reflectivity structure."""
    name = "hail_detect"
    version = "0.1.0"
    inputs = ["grid_ds_3d", "cell_stats"]      # keys from the published spec catalog
    outputs = ["hail_scores"]                   # spec contributed alongside, or table spec

    z_threshold_dbz: float = Field(55.0, description="Reflectivity threshold aloft")

    def compute(self, grid_ds_3d, cell_stats):  # named args bound from declared inputs
        ...
        return {"hail_scores": df}
```

```toml
[project.entry-points."adapt.modules"]
hail_detect = "adapt_hail.module:HailDetect"
```

Everything else — registration, DAG placement, validation, persistence, provenance,
config exposure in `config.yaml`, CLI selection — is framework-derived. Note
`compute(self, grid_ds_3d, cell_stats)` rather than `run(self, context)`: binding
declared inputs to named parameters removes the context dict from the contributor's
world entirely; the dict remains an engine internal. `adapt.sdk` is a *re-export
namespace* (Module, Field, testing helpers), not a new layer — one import line to
remember.

## Scaffolding and testing workflow

- `adapt module new hail_detect --inputs grid_ds_3d,cell_stats --table hail_scores`
  generates the package above plus a passing test, from the same template as the
  extension cookiecutter (Doc 02). The `.claude` skill then drives the official
  scaffold rather than maintaining a parallel recipe.
- `adapt.testing` ships the harness so a contributor never builds context dicts:

  ```python
  def test_hail_scores():
      result = run_module(HailDetect(z_threshold_dbz=50.0),
                          grid_ds_3d=make("grid_ds_3d", cells=2),  # spec-generated synthetic data
                          cell_stats=make("cell_stats", cells=2))
      assert (result["hail_scores"].score <= 1).all()
  ```

  `run_module` validates input/output specs exactly as the engine would — so a green
  unit test *is* evidence of pipeline compatibility, which today only an integration
  run provides.
- `adapt module lint` (or just the existing `pytest tests/test_architecture.py` +
  `lint-imports`, wrapped) gives one command answering "will the framework accept
  this?"

## Onboarding and documentation

- **The 15-minute tutorial** replaces the 306-line guide as the entry point: install →
  scaffold → edit `compute` → run on bundled sample scans → see your table in the
  dashboard. The current guide becomes reference material for module *internals*.
- **Spec catalog page** (auto-generated from DataSpecs, Doc 05): every context key,
  its schema, which modules produce/consume it. This single page eliminates the most
  common contributor question.
- **Contribution ladder**, stated in CONTRIBUTING.md: (1) your own repo with the
  template — zero review burden on the core team; (2) `adapt-contrib` distribution —
  light review, lower stability bar; (3) core distribution — full review. Most
  contributions should *stay* at rung 1 forever; that is success, not failure
  (Doc 10).
- **Metric**: time from `pip install` to a passing custom-module test, measured with
  real scientists. Target < 1 hour; today, honestly, it is a day-plus including
  guide-reading.

## Scoring the target workflow

| Criterion | Score | Rationale |
|-----------|-------|-----------|
| Contributor friendliness | High | One file, one class, named-arg `compute`; no orchestration concepts |
| Extensibility | High | Same path in-tree and third-party; ladder gives growth without gatekeeping |
| Testability | High | Spec-generated fixtures + `run_module` harness; green test ⇒ pipeline-compatible |
| Reproducibility | High | Module version mandatory in class; flows into manifest automatically |
| Operational complexity | Low | Scaffold + harness are dev-time tools; nothing added at runtime |
| Long-term maintainability | High | Boilerplate eliminated rather than generated; guide shrinks instead of growing |

**Answer to the critical question, restated:** today no — the obstacle is orchestration
knowledge (node wrappers, phase ints, config slicing, YAML registration) leaking into
the contribution path; after the merge/entry-points/specs work, yes — `compute(inputs)
-> outputs` plus one entry-point line is the entire contract.
