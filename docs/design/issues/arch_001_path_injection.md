# ARCH-001: Modules must not compute output paths

> **RESOLVED (2026-08-21, data store redesign).** Modules no longer see any
> path: raw files enter through the acquisition gateway (`StoreAcquirer`), all
> outputs go through `StoreOutputRouter` into the content-addressed object
> store, and `output_dirs` no longer exists. The text below is kept as the
> historical problem statement.

## Problem

Path construction is currently scattered across the wrong layers:

- `acquisition/_get_local_path()` inlines `output_dirs["base"] / radar / "nexrad" / date / filename`
- `ingest/LoadModule.run()` inlines `output_dirs["base"] / radar / "gridnc" / date / filename`
- `directories.get_plot_path()` creates the plots directory and returns a path — called via lazy import from `plotter.py`

Modules know the directory structure. They should not. A module's job is science: data in, data out. The path where output lands is an orchestration decision.

## Why it matters

- Changing the directory layout requires editing module internals.
- Modules cannot be unit-tested without a real filesystem layout.
- The same path pattern is duplicated in multiple places with no single source of truth.

## Correct architecture

The runtime / configuration layer owns all paths. Modules receive pre-computed paths (or output directories) as named context inputs:

```
# Processor builds context before calling executor:
context["nexrad_save_dir"] = output_dirs["base"] / radar / "nexrad" / date_str
context["gridnc_save_dir"] = output_dirs["base"] / radar / "gridnc" / date_str

# Module declares it as an input:
class LoadModule(BaseModule):
    inputs = ["nexrad_file", "ingest_config", "gridnc_save_dir"]
```

`setup_output_directories` and all path construction stay in `configuration/` or `runtime/`. Modules receive `Path` objects, never construct them.

## Scope

- `src/adapt/modules/acquisition/module.py` — remove `_get_local_path` path construction; receive save dir from context or config
- `src/adapt/modules/ingest/module.py` — remove inline path construction; receive `gridnc_save_dir` from context
- `src/adapt/configuration/schemas/directories.py` — `get_plot_path` should move to `runtime/` or `visualization/`; `directories.py` should only contain `setup_output_directories`
- `src/adapt/runtime/processor.py` — compute and inject all output paths into context before calling executors
