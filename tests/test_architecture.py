# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Architecture tests: enforce module-independence without hardcoding module names.

These tests discover adapt.modules subpackages at runtime and verify that
no scientific module imports from any other scientific module. New modules
are picked up automatically — no test edits required.

Run: pytest tests/test_architecture.py
"""

import ast
import importlib
import pkgutil
from pathlib import Path

import pytest

# Skip the entire file gracefully if adapt is not installed in this environment.
# This prevents VSCode pytest discovery errors when the wrong interpreter is active.
adapt_modules = pytest.importorskip(
    "adapt.modules",
    reason="adapt not installed in this Python environment — activate adapt_env",
)


def _discover_module_packages() -> list[str]:
    """Return all immediate subpackage names under adapt.modules."""
    return [
        f"adapt.modules.{info.name}"
        for info in pkgutil.iter_modules(adapt_modules.__path__)
        if info.ispkg
    ]


def _source_files(package_name: str) -> list[Path]:
    """Return all .py files belonging to a package."""
    mod = importlib.import_module(package_name)
    assert mod.__file__ is not None
    pkg_dir = Path(mod.__file__).parent
    return list(pkg_dir.rglob("*.py"))


def _imported_adapt_modules(py_file: Path) -> set[str]:
    """Parse a .py file and return the set of adapt.modules.* names it imports."""
    try:
        tree = ast.parse(py_file.read_text())
    except SyntaxError:
        return set()

    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("adapt.modules."):
                    imports.add(alias.name)
        elif (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("adapt.modules.")
        ):
            imports.add(node.module)
    return imports


# Build the test matrix at collection time — works for any future module.
_PACKAGES = _discover_module_packages()
_SKIP = {"adapt.modules.base"}  # base.py is shared infrastructure, not a science module


@pytest.mark.parametrize("pkg", [p for p in _PACKAGES if p not in _SKIP])
def test_module_does_not_import_other_modules(pkg: str) -> None:
    """Scientific module must not import from any other adapt.modules subpackage.

    This test is parameterised over every subpackage discovered under adapt.modules.
    Adding a new module directory makes it appear here automatically.
    """
    files = _source_files(pkg)
    violations: list[str] = []

    for py_file in files:
        for imported in _imported_adapt_modules(py_file):
            # Allow self-imports (within the same subpackage)
            if not imported.startswith(pkg):
                violations.append(f"  {py_file.name}: imports {imported!r}")

    assert not violations, (
        f"\n{pkg} imports from other scientific modules — "
        "shared types belong in adapt.contracts:\n" + "\n".join(violations)
    )


@pytest.mark.parametrize("pkg", [p for p in _PACKAGES if p not in _SKIP])
def test_module_does_not_import_execution_or_runtime(pkg: str) -> None:
    """Scientific module must not import from adapt.execution or adapt.runtime."""
    forbidden_prefixes = ("adapt.execution", "adapt.runtime", "adapt.persistence")
    files = _source_files(pkg)
    violations: list[str] = []

    for py_file in files:
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for name in names:
                if any(name.startswith(p) for p in forbidden_prefixes):
                    violations.append(f"  {py_file.name}: imports {name!r}")

    assert not violations, (
        f"\n{pkg} imports from layers above it — "
        "modules must only depend on contracts/ and utils/:\n" + "\n".join(violations)
    )


# ── Canonical scan-time serialization (single source of truth) ────────────────
# scan_time is the cross-table join key: cells_by_scan and every derived module
# table must store the identical string, or joins silently fail. The format lives
# in exactly one function — adapt.utils.time.to_scan_iso. This fitness function
# fails if any other code formats scan-time independently (drift = broken joins).

_SCAN_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_SRC_ADAPT = Path(__file__).parents[1] / "src" / "adapt"


def test_scan_time_format_is_defined_in_exactly_one_place() -> None:
    """The scan-time join-key format may appear only in adapt.utils.time."""
    offenders: list[str] = []
    for py_file in _SRC_ADAPT.rglob("*.py"):
        if _SCAN_TIME_FORMAT in py_file.read_text():
            offenders.append(str(py_file.relative_to(_SRC_ADAPT)))

    assert offenders == ["utils/time.py"], (
        "scan-time format must be centralized in adapt.utils.time.to_scan_iso — "
        f"found the literal {_SCAN_TIME_FORMAT!r} in: {offenders}. "
        "Serialize scan_time via to_scan_iso (or let ModuleOutputWriter do it); "
        "never hardcode the format, or derived tables will not join cells_by_scan."
    )


# ── Determinism: no wall clock or global RNG in scientific modules ─────────────
# Identical inputs + config must produce identical outputs. A module that reads
# the wall clock or numpy's global RNG breaks that silently. The acquisition
# module is the single allowed exception: it injects its clock (testable) and
# stamps queue telemetry, which never enters scientific outputs.

_WALL_CLOCK_ALLOWED = {"acquisition/module.py"}


def _nondeterminism_calls(py_file: Path) -> list[str]:
    """Return wall-clock / global-RNG usages in a file (AST, ignores docstrings)."""
    tree = ast.parse(py_file.read_text())
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        value = node.value
        if isinstance(value, ast.Name):
            if value.id == "datetime" and node.attr in {"now", "utcnow", "today"}:
                offenders.append(f"line {node.lineno}: datetime.{node.attr}")
            elif value.id == "time" and node.attr == "time":
                offenders.append(f"line {node.lineno}: time.time")
        elif (
            isinstance(value, ast.Attribute)
            and value.attr == "random"
            and isinstance(value.value, ast.Name)
            and value.value.id in {"np", "numpy"}
        ):
            offenders.append(f"line {node.lineno}: np.random.{node.attr}")
    return offenders


@pytest.mark.parametrize("pkg", [p for p in _PACKAGES if p not in _SKIP])
def test_module_does_not_read_wall_clock_or_global_rng(pkg: str) -> None:
    """Scientific modules must be deterministic: no wall clock, no global RNG."""
    modules_dir = _SRC_ADAPT / "modules"
    violations: list[str] = []

    for py_file in _source_files(pkg):
        rel = str(py_file.relative_to(modules_dir))
        if rel in _WALL_CLOCK_ALLOWED:
            continue
        violations.extend(f"  {rel}: {hit}" for hit in _nondeterminism_calls(py_file))

    assert not violations, (
        f"\n{pkg} reads the wall clock or global RNG — identical inputs must give "
        "identical outputs. Inject a clock (see acquisition/module.py) or seed an "
        "explicit np.random.default_rng from config:\n" + "\n".join(violations)
    )


# ── Heavy third-party dependencies stay in their owning component ──────────────
# Each heavy or domain-specific dependency is imported by exactly one component.
# If one leaks (e.g. matplotlib into modules/, cv2 into runtime/), the core
# becomes uninstallable headless and modules stop being swappable.

_DEP_HOMES = {
    "matplotlib": ("visualization/", "consumers/"),
    "contextily": ("visualization/", "consumers/"),
    "tkinter": ("consumers/",),
    "cv2": ("modules/projection/",),
    "pyart": ("modules/ingest/",),
    "nexradaws": ("modules/acquisition/",),
    "networkx": ("modules/tracking/",),
    "duckdb": ("api/",),
    "pyxlma": ("modules/xlma_stat/",),
    "sklearn": ("modules/xlma_stat/",),
    "skimage": ("modules/detection/", "modules/analysis/"),
    "pyproj": ("modules/xlma_stat/", "consumers/"),
}


def _top_level_imports(py_file: Path) -> set[str]:
    tree = ast.parse(py_file.read_text())
    tops: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            tops.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            tops.add(node.module.split(".")[0])
    return tops


@pytest.mark.parametrize("dep", sorted(_DEP_HOMES))
def test_heavy_dependency_stays_in_its_component(dep: str) -> None:
    """Each heavy dependency may only be imported inside its owning component."""
    allowed = _DEP_HOMES[dep]
    violations: list[str] = []

    for py_file in _SRC_ADAPT.rglob("*.py"):
        rel = str(py_file.relative_to(_SRC_ADAPT))
        if dep in _top_level_imports(py_file) and not rel.startswith(allowed):
            violations.append(f"  {rel}")

    assert not violations, (
        f"\n'{dep}' may only be imported under {list(allowed)} — move the call "
        "behind that component's interface instead of importing directly:\n" + "\n".join(violations)
    )


# ── Context-dict ownership: declared IO must be coherent ───────────────────────
# The context dict is the inter-module interface. Two modules claiming the same
# output key would shadow each other; an input nobody produces and nobody seeds
# is a typo that only fails at runtime. These tests make both fail at CI time.

# Keys injected by the runtime (processor/orchestrator) before the graph runs,
# i.e. legitimate module inputs that no module produces.
_CONTEXT_SEED_KEYS = frozenset(
    {
        "nexrad_file",  # queued file path from the source
        "scan_history",  # rolling window of prior segmented scans
        "grid_ds_3d",  # full 3D grid sliced in by the processor
        "run_id",  # repository run identifier
        "scan_time",  # ingest outputs it; processor re-seeds it in phase 2
        "ingest_config",
        "detection_config",
        "projection_config",
        "analysis_config",
        "tracking_config",
        "cell_volume_stats_config",
    }
)


def _registered_default_modules():
    """Live pipeline modules only — postprocess modules have their own injector.

    Post-process modules (pipeline_phase == POSTPROCESS_PHASE) receive
    repository-backed inputs from the PostProcessor on demand, so their inputs
    are validated by the postprocessor tests, not by this seed-key registry.
    """
    from adapt.execution.module_registry import registry
    from adapt.execution.pipeline_builder import _ensure_modules_registered
    from adapt.modules.base import POSTPROCESS_PHASE

    _ensure_modules_registered()
    return [m for m in registry.create_modules() if m.pipeline_phase != POSTPROCESS_PHASE]


def test_module_output_keys_are_unique() -> None:
    """No two registered modules may declare the same output key."""
    producers: dict[str, str] = {}
    collisions: list[str] = []

    for module in _registered_default_modules():
        for key in module.outputs:
            if key in producers:
                collisions.append(f"  '{key}': {producers[key]} and {module.name}")
            else:
                producers[key] = module.name

    assert not collisions, (
        "\nOutput-key collision in the context dict — later modules would shadow "
        "earlier results:\n" + "\n".join(collisions)
    )


def test_module_inputs_are_produced_or_seeded() -> None:
    """Every declared input is another module's output or a runtime-seeded key."""
    modules = _registered_default_modules()
    produced = {key for m in modules for key in m.outputs}
    orphans: list[str] = []

    for module in modules:
        for key in module.inputs:
            if key not in produced and key not in _CONTEXT_SEED_KEYS:
                orphans.append(f"  {module.name} needs '{key}'")

    assert not orphans, (
        "\nInput key is neither produced by any module nor seeded by the runtime — "
        "either a typo in the declaration or _CONTEXT_SEED_KEYS needs the new "
        "runtime-injected key added here, with a comment saying who seeds it:\n"
        + "\n".join(orphans)
    )


# Outputs knowingly shipped without a contract validator. This list may only
# shrink: never add to it — write a check_* validator in adapt.contracts instead.
_UNCONTRACTED_OUTPUTS = {
    ("ingest", "grid_ds"),
    ("ingest", "scan_time"),
    ("ingest", "grid_nc_path"),
    ("detection", "num_cells"),
}


def test_module_outputs_carry_contracts() -> None:
    """Every module output has a validator, except the pinned shrink-only list."""
    uncontracted = {
        (module.name, key)
        for module in _registered_default_modules()
        for key in module.outputs
        if key not in (module.output_contracts or {})
    }

    new = uncontracted - _UNCONTRACTED_OUTPUTS
    assert not new, (
        f"\nNew module outputs without contracts: {sorted(new)}. "
        "Add a check_* validator to adapt.contracts and register it in "
        "output_contracts — do not extend _UNCONTRACTED_OUTPUTS."
    )

    stale = _UNCONTRACTED_OUTPUTS - uncontracted
    assert not stale, (
        f"\n_UNCONTRACTED_OUTPUTS contains entries that now have contracts: "
        f"{sorted(stale)}. Remove them so the ratchet only tightens."
    )


# ── Telemetry ids stay out of the science context dict ────────────────────────
# Observability correlation ids (trace/span/scan/pipeline/...) travel out-of-band
# in contextvars. If one ever appeared as a module input/output key it would couple
# telemetry to the science data contract and could perturb determinism. Pin it.


def test_obs_context_fields_never_in_module_io() -> None:
    """No ObsContext field name may be a module input/output key."""
    from adapt.contracts.observability import ObsContext

    obs_fields = set(ObsContext.__dataclass_fields__)
    leaks: list[str] = []
    for module in _registered_default_modules():
        for key in list(module.inputs) + list(module.outputs):
            if key in obs_fields:
                leaks.append(f"  {module.name}: '{key}'")

    assert not leaks, (
        "\nTelemetry correlation ids leaked into the science context dict — keep "
        "ObsContext fields out of module inputs/outputs:\n" + "\n".join(leaks)
    )
