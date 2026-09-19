# Execution Graph

**Location**: `src/adapt/execution/`

---

## Responsibility

The execution graph is the **wiring and sequencing engine** of the pipeline. Its sole job is to take a set of declared modules, determine what order they must run in, execute them in that order, and verify their outputs.

It knows nothing about radar data, file formats, threading, or storage.

---

## What It Is Not Responsible For

- Deciding which modules exist — that is the registry's job
- Loading data from disk — that is the ingest module's job
- Writing data to disk — that is the persistence layer's job
- Running modules on multiple files — that is the runtime orchestrator's job
- Error recovery or retries — fail loudly and propagate

---

## Components

### Node (`graph/node.py`)

A thin wrapper around a module that tracks its position in the DAG.

```
Node
├── module: BaseModule          # The processing unit
├── name: str                   # Derived from module.name
├── inputs: list[str]           # Keys module reads from context
├── outputs: list[str]          # Keys module writes to context
├── dependencies: list[Node]    # Upstream nodes (auto-wired)
└── dependents: list[Node]      # Downstream nodes (auto-wired)
```

A node is a pure data structure. It has no `run()` method. Execution is the executor's concern.

### GraphBuilder (`graph/builder.py`)

Takes a flat list of module instances and produces a set of wired nodes.

**Algorithm**:
1. Create one node per module
2. Build `output_map: {key → node}` — maps each produced key to its producing node
3. For each node, for each of its `inputs`: look up the node in `output_map` and register as a dependency
4. Detect duplicate outputs across nodes — raise `ValueError` immediately
5. Detect unresolvable inputs (no producer found) — raise `ValueError` immediately

The builder does not run any module. It only wires.

**Invariant**: After build, the node graph is a valid DAG — no cycles, no missing producers. This is verified before any execution begins.

### GraphExecutor (`graph/executor.py`)

Takes the wired nodes and an initial context dictionary, runs all nodes in dependency order, and returns the final context.

**Algorithm** (topological execution via iterative pass):
1. Mark all nodes `pending`
2. Loop (max iterations = n² + n + 1 to detect infinite loops):
   a. For each pending node, check if all dependencies are `completed`
   b. If yes: validate inputs via `input_contracts`, call `module.run(context)`, merge output into context, validate outputs via `output_contracts`, mark `completed`
   c. If no nodes completed in an iteration: the graph is stalled — raise `RuntimeError`
3. Return final context when all nodes `completed`

**Contract enforcement** happens here, not in the modules themselves. The executor calls the validators declared in `module.input_contracts` and `module.output_contracts` before and after each run. This keeps modules free of boilerplate validation logic.

### ModuleRegistry (`module_registry.py`)

A singleton that holds references to registered module classes (not instances). At pipeline construction time, it instantiates each class once.

**Pattern**:
```python
# In any module file (at import time):
from adapt.execution.module_registry import registry
registry.register(IngestModule)

# In pipeline builder:
modules = registry.create_modules()  # Returns fresh instances
```

This is the **Open/Closed Principle in practice**: registering a new module requires zero modifications to the executor, the pipeline builder, or any other module. The registry is the single point of assembly.

**Thread Safety**: The registry is populated at import time before any threads start. After pipeline construction, it is read-only.

### NexradPipeline (`pipeline_builder.py`)

The entry point that assembles one fully wired, ready-to-execute pipeline.

**Steps at construction time**:
1. Import module files listed in `defaults.yaml` (triggers `registry.register(...)` side effects)
2. Call `registry.create_modules()` to get fresh module instances
3. Call `GraphBuilder(modules).build()` to produce wired nodes
4. Create `GraphExecutor(nodes)` and store it

**Per-file execution**:
```python
pipeline.process_file(filepath, context)
→ executor.run(context)
→ returns final context
```

Module instances persist across files. This is intentional: `ProjectionModule` maintains frame history between consecutive scans. The module owns its own state; the executor does not touch it.

---

## Context Dictionary Contract

The context is a plain Python `dict`. Keys are string identifiers. Values are domain objects (xarray Datasets, DataFrames, file paths, config objects).

**Initial context** (injected by runtime before execution):
```
nexrad_file    → Path to raw Level-II file
config         → InternalConfig (frozen)
run_id         → the run this execution belongs to
```

**After full pipeline execution**:
```
grid_ds        → xarray.Dataset (3D Cartesian grid)
grid_ds_2d     → xarray.Dataset (2D z-slice)
segmented_ds   → xarray.Dataset (grid_ds_2d + cell_labels)
projected_ds   → xarray.Dataset (segmented_ds + heading + projections)
cell_stats     → pd.DataFrame (per-cell statistics)
tracked_cells  → pd.DataFrame (cell_stats + tracking fields)
cell_events    → pd.DataFrame (lineage edges)
scan_time      → datetime
```

No key is ever deleted from context. Downstream modules can always inspect upstream outputs.

---

## Extension: Adding a New Module

1. Implement `BaseModule` in `modules/<domain>/module.py`
2. Declare `name`, `inputs`, `outputs`
3. Optionally declare `input_contracts` and `output_contracts`
4. Register: `registry.register(MyModule)` at module-file import time
5. Add module file path to `defaults.yaml`

The graph will automatically re-wire. If the new module produces a key that an existing module needs, the existing module gets it. If the new module needs a key an existing module produces, the dependency is wired. No other changes required.

---

## Failure Modes

| Situation | Behaviour |
|-----------|-----------|
| Module produces wrong output schema | `ContractViolation` raised at the stage boundary |
| Two modules produce the same key | `ValueError` at `GraphBuilder.build()` |
| A module requires a key no one produces | `ValueError` at `GraphBuilder.build()` |
| Graph has a cycle | `RuntimeError` (max iterations exceeded) |
| Graph stalls (dependency never satisfied) | `RuntimeError` with stalled node names |
| Module raises an exception | Propagates immediately, no catch, no retry |
