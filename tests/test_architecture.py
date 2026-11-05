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
import re
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
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
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
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
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
# scan_id is the cross-table join key; scan_time is ordering/display METADATA —
# but it must still serialize identically everywhere (one canonical format), or
# time-ordered reads and displays silently disagree. The format lives in exactly
# one function — adapt.utils.time.to_scan_iso. This fitness function fails if
# any other code formats scan-time independently. (Evidence for the join-key
# change: the pre-identity dashboard failure class — wall-clock-corrupted rows,
# "+00:00" vs "Z" divergence, and ±60/±90 s consumer tolerance windows.)

_SCAN_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_SRC_ADAPT = Path(__file__).parents[1] / "src" / "adapt"


def _rel(py_file: Path, root: Path) -> str:
    """Path of *py_file* relative to *root*, always with '/' separators.

    These fitness functions compare paths against literal module locations, so
    the separator must not depend on the OS the suite runs on.
    """
    return py_file.relative_to(root).as_posix()


def test_scan_time_format_is_defined_in_exactly_one_place() -> None:
    """The canonical scan-time metadata format may appear only in adapt.utils.time."""
    offenders: list[str] = []
    for py_file in _SRC_ADAPT.rglob("*.py"):
        if _SCAN_TIME_FORMAT in py_file.read_text(encoding="utf-8"):
            offenders.append(_rel(py_file, _SRC_ADAPT))

    assert offenders == ["utils/time.py"], (
        "scan-time format must be centralized in adapt.utils.time.to_scan_iso — "
        f"found the literal {_SCAN_TIME_FORMAT!r} in: {offenders}. "
        "Serialize scan_time via to_scan_iso (or let ModuleOutputWriter do it); "
        "never hardcode the format. Rows JOIN on scan_id; scan_time is the "
        "single-format ordering/display metadata."
    )


def test_adapt_name_is_never_all_caps() -> None:
    """The product is 'Adapt', never 'ADAPT' — in output, prints, comments, or docstrings.

    Matches the standalone token only, so 'arm_adaptive', 'ADAPTIVE', 'ADAPTER' are fine.
    """
    offenders = [
        f"{_rel(py_file, _SRC_ADAPT)}:{i}"
        for py_file in _SRC_ADAPT.rglob("*.py")
        for i, line in enumerate(py_file.read_text(encoding="utf-8").splitlines(), 1)
        if re.search(r"\bADAPT\b", line)
    ]
    assert not offenders, "Use 'Adapt', not 'ADAPT':\n" + "\n".join(offenders)


# ── Determinism: no wall clock or global RNG in scientific modules ─────────────
# Identical inputs + config must produce identical outputs. A module that reads
# the wall clock or numpy's global RNG breaks that silently. The acquisition
# module is the single allowed exception: it injects its clock (testable) and
# stamps queue telemetry, which never enters scientific outputs.

_WALL_CLOCK_ALLOWED = {"acquisition/module.py"}


def _nondeterminism_calls(py_file: Path) -> list[str]:
    """Return wall-clock / global-RNG usages in a file (AST, ignores docstrings)."""
    tree = ast.parse(py_file.read_text(encoding="utf-8"))
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
        rel = _rel(py_file, modules_dir)
        if rel in _WALL_CLOCK_ALLOWED:
            continue
        violations.extend(f"  {rel}: {hit}" for hit in _nondeterminism_calls(py_file))

    assert not violations, (
        f"\n{pkg} reads the wall clock or global RNG — identical inputs must give "
        "identical outputs. Inject a clock (see acquisition/module.py) or seed an "
        "explicit np.random.default_rng from config:\n" + "\n".join(violations)
    )


def test_execution_nodes_do_not_read_wall_clock() -> None:
    """Graph nodes must be deterministic too, not just modules/*.

    The pre-identity wall-clock scan_time fallback lived in
    execution/nodes/ingest.py — exactly where the per-module fitness function
    above does not look. Scan times come from the source boundary; nothing in
    the execution layer may substitute a clock.
    """
    execution_dir = _SRC_ADAPT / "execution"
    violations: list[str] = []
    for py_file in execution_dir.rglob("*.py"):
        violations.extend(
            f"  {_rel(py_file, execution_dir)}: {hit}" for hit in _nondeterminism_calls(py_file)
        )

    assert not violations, (
        "\nadapt.execution reads the wall clock or global RNG — scan times are "
        "owned by the source boundary and identical inputs must give identical "
        "outputs:\n" + "\n".join(violations)
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
    "pyart": ("modules/ingest/", "modules/detection/"),
    "boto3": ("downloaders/",),
    "botocore": ("downloaders/",),
    "networkx": ("modules/tracking/",),
    "pyxlma": ("modules/xlma_stat/",),
    "sklearn": ("modules/xlma_stat/",),
    "skimage": ("modules/detection/", "modules/analysis/"),
    "pyproj": ("modules/xlma_stat/", "consumers/"),
}


def _top_level_imports(py_file: Path) -> set[str]:
    tree = ast.parse(py_file.read_text(encoding="utf-8"))
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
        rel = _rel(py_file, _SRC_ADAPT)
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
        "prior_scan",  # previous completed scan context; processor seeds it (None when unusable)
        "grid_ds_3d",  # full 3D grid sliced in by the processor
        "run_id",  # repository run identifier
        "scan_id",  # sha256[:16] of raw bytes; processor seeds it every scan (uid-v2 birth input)
        "scan_time",  # owned by the source boundary; processor seeds it every scan
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


# ── Architecture doc, import-linter config, and source tree stay in sync ──────
# ARCHITECTURE.md is the human/agent-facing contract; .importlinter is the
# machine-enforced one; src/adapt is reality. These fitness functions fail the
# moment any of the three drifts — the exact failure mode of the pre-audit
# AGENTS.md, which described an architecture that no longer existed.

_REPO_ROOT = Path(__file__).parents[1]
_ARCHITECTURE_MD = _REPO_ROOT / "ARCHITECTURE.md"
_IMPORTLINTER = _REPO_ROOT / ".importlinter"


def _doc_layer_lines() -> list[str]:
    """The lines of the ```layers fenced block in ARCHITECTURE.md."""
    text = _ARCHITECTURE_MD.read_text(encoding="utf-8")
    match = re.search(r"```layers\n(.*?)```", text, re.DOTALL)
    assert match, "ARCHITECTURE.md must contain a ```layers fenced block"
    return [line.strip() for line in match.group(1).splitlines() if line.strip()]


def _importlinter_field(field: str) -> list[str]:
    """Indented entries of a `field =` list in the .importlinter layers contract."""
    lines = _IMPORTLINTER.read_text(encoding="utf-8").splitlines()
    entries: list[str] = []
    in_field = False
    for line in lines:
        if line.strip() == f"{field} =":
            in_field = True
            continue
        if in_field:
            if line.startswith((" ", "\t")) and line.strip():
                entries.append(line.strip())
            else:
                break
    return entries


def test_architecture_md_layers_match_importlinter() -> None:
    """The layer stack in ARCHITECTURE.md is identical to the one lint-imports enforces."""
    assert _doc_layer_lines() == _importlinter_field("layers"), (
        "ARCHITECTURE.md's ```layers block and .importlinter's `layers =` list "
        "have diverged — update both in the same change."
    )


def test_layer_stack_covers_the_real_source_tree() -> None:
    """Every top-level package/module under src/adapt is placed in a layer
    (or deliberately listed in exhaustive_ignores). lint-imports enforces this
    too; this test keeps the guarantee even where only pytest runs."""
    placed = {name for line in _doc_layer_lines() for name in line.split("|")}
    placed = {name.strip() for name in placed}
    ignored = set(_importlinter_field("exhaustive_ignores"))

    actual = {
        p.name
        for p in _SRC_ADAPT.iterdir()
        if p.is_dir() and (p / "__init__.py").exists() and p.name != "__pycache__"
    } | {p.stem for p in _SRC_ADAPT.glob("*.py") if p.stem != "__init__"}

    assert placed | ignored == actual, (
        f"Layer stack + exhaustive_ignores != src/adapt tree.\n"
        f"  unplaced packages: {sorted(actual - placed - ignored)}\n"
        f"  phantom entries:   {sorted((placed | ignored) - actual)}\n"
        "Place new packages in a layer in BOTH ARCHITECTURE.md and .importlinter."
    )
    assert not placed & ignored, (
        f"{sorted(placed & ignored)} appear both in the layer stack and in "
        "exhaustive_ignores — pick one."
    )


# ── DDL single home: static CREATE TABLE lives in configuration/schemas/*.sql ──
# The store's bookkeeping tables are defined once, in the .sql schema files.
# api/store_client.py queries these tables by name, so every extra place that
# defines them is another chance for silent reader/writer drift. The allowlist
# below pins today's exceptions and may only SHRINK — move DDL into the .sql
# files (or through SchemaLedger.freeze), never into a new Python file.

_DDL_ALLOWED_PY = {
    # The sanctioned dynamic creator: frozen first-frame product tables.
    "persistence/products.py",
    # Legacy bespoke DDL — shrink-only; consolidate into schemas/*.sql.
    "persistence/track_store.py",
}

# Case-sensitive: SQL here is uppercase by convention; lowercase "Create
# tables" in docstring prose must not count.
_CREATE_TABLE = re.compile(r"CREATE\s+TABLE")


def test_create_table_statements_have_one_home() -> None:
    """CREATE TABLE may appear only in configuration/schemas/*.sql or the pinned files."""
    offenders = [
        rel
        for py_file in _SRC_ADAPT.rglob("*.py")
        if _CREATE_TABLE.search(py_file.read_text(encoding="utf-8"))
        and (rel := _rel(py_file, _SRC_ADAPT)) not in _DDL_ALLOWED_PY
    ]
    assert not offenders, (
        f"\nCREATE TABLE outside its home: {offenders}. Table definitions live in "
        "configuration/schemas/*.sql (static tables) or go through "
        "SchemaLedger.freeze (dynamic product tables) — do not extend _DDL_ALLOWED_PY."
    )

    stale = {
        rel
        for rel in _DDL_ALLOWED_PY
        if not _CREATE_TABLE.search((_SRC_ADAPT / rel).read_text(encoding="utf-8"))
    }
    assert not stale, (
        f"\n_DDL_ALLOWED_PY contains files with no CREATE TABLE left: {sorted(stale)}. "
        "Remove them so the ratchet only tightens."
    )


# ── Field-generic core: reflectivity stat-column literals are quarantined ─────
# After the tracking_field refactor, stat columns are minted via
# adapt.contracts.stat_column and read via the configured field. The literal
# "radar_reflectivity_max" may appear only in files still awaiting phase-3
# consumer role resolution, plus the fixed cells_by_scan DDL. Shrink-only.

_REFL_LITERAL_ALLOWED = {
    "persistence/track_store.py",  # _CBS_FIXED_COLUMNS (fixed DDL)
}


def test_reflectivity_stat_literal_is_quarantined() -> None:
    offenders = [
        rel
        for py_file in _SRC_ADAPT.rglob("*.py")
        if "radar_reflectivity_max" in py_file.read_text(encoding="utf-8")
        and (rel := _rel(py_file, _SRC_ADAPT)) not in _REFL_LITERAL_ALLOWED
    ]
    assert not offenders, (
        f"\n'radar_reflectivity_max' literal outside the quarantine: {offenders}. "
        "Mint stat columns via adapt.contracts.stat_column and resolve the "
        "field from config/provenance."
    )
    stale = {
        rel
        for rel in _REFL_LITERAL_ALLOWED
        if "radar_reflectivity_max" not in (_SRC_ADAPT / rel).read_text(encoding="utf-8")
    }
    assert not stale, (
        f"\n_REFL_LITERAL_ALLOWED has stale entries: {sorted(stale)} — "
        "remove them so the ratchet only tightens."
    )


# ── Telemetry ids stay out of the science context dict ────────────────────────
# Observability correlation ids (trace/span/scan/pipeline/...) travel out-of-band
# in contextvars. If one ever appeared as a module input/output key it would couple
# telemetry to the science data contract and could perturb determinism. Pin it.


def test_obs_context_fields_never_in_module_io() -> None:
    """No ObsContext field name may be a module input/output key.

    Runtime-seeded SCIENCE identity keys are exempt: ``scan_id`` is the
    store join key (sha256 of raw bytes) and the uid-v2 birth input —
    ObsContext.scan_id is a correlation id DERIVED from it, not the
    reverse, so a module declaring it consumes science identity, not
    telemetry.
    """
    from adapt.contracts.observability import ObsContext

    obs_fields = set(ObsContext.__dataclass_fields__) - _CONTEXT_SEED_KEYS
    leaks: list[str] = []
    for module in _registered_default_modules():
        for key in list(module.inputs) + list(module.outputs):
            if key in obs_fields:
                leaks.append(f"  {module.name}: '{key}'")

    assert not leaks, (
        "\nTelemetry correlation ids leaked into the science context dict — keep "
        "ObsContext fields out of module inputs/outputs:\n" + "\n".join(leaks)
    )


# ── Consumer boundary: consumers read ONLY through the store API ───────────────
# Consumers (dashboard, TSE) may import adapt.api + adapt.utils only; the store
# fitness below closes the I/O side doors the import-linter cannot see: raw
# sqlite, direct NetCDF/parquet opens, directory walks, and store-internal
# path construction. Rasters arrive in-memory via the client; tables arrive as
# DataFrames. The one sanctioned literal is the registry.db root marker used
# for repo detection in _utils.
_CONSUMER_FORBIDDEN_CALLS = {
    "open_dataset",
    "open_mfdataset",
    "load_dataset",
    "read_parquet",
    "glob",
    "rglob",
    "iterdir",
    "connect",  # sqlite3.connect / duckdb.connect
}
_CONSUMER_FORBIDDEN_LITERALS = ("catalog.db", "products.db", "adapt_registry.db")
_CONSUMER_LITERAL_ALLOWLIST = {"_utils.py"}  # legacy-root detection marker only


def _consumer_violations(py_file: Path) -> list[str]:
    import ast

    tree = ast.parse(py_file.read_text(encoding="utf-8"))
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in {"sqlite3", "duckdb"}:
                    violations.append(f"{py_file.name}:{node.lineno} imports {alias.name}")
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[0] in {"sqlite3", "duckdb"}:
                violations.append(f"{py_file.name}:{node.lineno} imports {node.module}")
        elif isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if name in _CONSUMER_FORBIDDEN_CALLS:
                violations.append(f"{py_file.name}:{node.lineno} calls {name}()")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and any(lit in node.value for lit in _CONSUMER_FORBIDDEN_LITERALS)
            and py_file.name not in _CONSUMER_LITERAL_ALLOWLIST
        ):
            violations.append(f"{py_file.name}:{node.lineno} builds store path {node.value!r}")
    return violations


def test_consumers_read_only_through_the_store_api() -> None:
    """No consumer touches storage directly — every read goes through StoreClient.

    Guards the exact failure classes behind past dashboard bugs: raw sqlite
    connections (fd leaks), per-frame NetCDF opens (too many open files),
    directory globs (stale/foreign files), and hardcoded store paths.
    """
    consumers_dir = Path(__file__).parent.parent / "src" / "adapt" / "consumers"
    violations: list[str] = []
    for py_file in sorted(consumers_dir.rglob("*.py")):
        violations.extend(_consumer_violations(py_file))
    assert not violations, "consumers must read via the store API only:\n" + "\n".join(violations)
