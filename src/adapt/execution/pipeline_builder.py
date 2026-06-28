# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Module registration and selection for pipeline assembly.

``_ensure_modules_registered`` imports every module path declared in
``configuration/defaults.yaml`` (plus user extensions), which triggers each
module's ``registry.register()`` call. ``resolve_enabled_modules`` filters the
registered set down to the enabled modules for a run.

Both fail loudly: an unreadable module list or a module that cannot be
imported aborts startup — there is no fallback pipeline.
"""

import importlib
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_DEFAULTS_YAML = Path(__file__).parent.parent / "configuration" / "defaults.yaml"


def _ensure_modules_registered(extensions: list[str] | None = None) -> None:
    """Import module files listed in config/defaults.yaml plus any user extensions.

    Each entry under ``pipeline.modules`` is a Python module path.
    Importing it triggers the ``registry.register()`` call at module level.
    To add a core module: add one line to defaults.yaml.
    To add an extension: pass its dotted import path via ``extensions``.

    Raises
    ------
    RuntimeError
        If defaults.yaml is unreadable or declares no modules.
    ImportError
        If any declared core module or extension fails to import.
    """
    try:
        with open(_DEFAULTS_YAML, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except OSError as e:
        raise RuntimeError(f"Cannot read pipeline module list '{_DEFAULTS_YAML}': {e}") from e

    module_paths = (cfg or {}).get("pipeline", {}).get("modules", [])
    if not module_paths:
        raise RuntimeError(
            f"No pipeline modules declared under 'pipeline.modules' in '{_DEFAULTS_YAML}'"
        )

    for path in module_paths:
        try:
            importlib.import_module(path)
            logger.debug("Registered core module from: %s", path)
        except Exception as e:
            raise ImportError(f"Failed to import core module '{path}': {e}") from e

    for path in extensions or []:
        try:
            importlib.import_module(path)
            logger.info("Registered extension module from: %s", path)
        except Exception as e:
            raise ImportError(f"Failed to load extension module '{path}': {e}") from e


def resolve_enabled_modules(
    all_modules: list,
    modules: list[str] | None = None,
    only: list[str] | None = None,
    exclude: list[str] | None = None,
) -> list:
    """Filter ``all_modules`` to the enabled set, preserving order.

    Precedence (CLI over file): start from every module; if ``modules`` (the config
    allowlist) is given, restrict to it; then ``only`` restricts further (exact set)
    or ``exclude`` subtracts. Every referenced name must be a real module. After
    filtering, validates that no enabled module needs an input produced solely by a
    disabled module — raising a clear error rather than failing opaquely at runtime.

    Parameters
    ----------
    all_modules : list
        All registered module instances (each with ``name``/``inputs``/``outputs``).
    modules, only, exclude : list[str], optional
        Config allowlist, ``--only`` set, and ``--not`` set respectively.
    """
    by_name = {m.name: m for m in all_modules}
    for label, names in (("modules", modules), ("--only", only), ("--not", exclude)):
        for n in names or []:
            if n not in by_name:
                raise ValueError(
                    f"Unknown module '{n}' in {label}. Available: {', '.join(by_name)}"
                )

    enabled = set(by_name)
    if modules is not None:
        enabled = set(modules)
    if only:
        enabled = set(only)
    elif exclude:
        enabled -= set(exclude)

    producer: dict[str, str] = {}
    for m in all_modules:
        for out in m.outputs:
            producer[out] = m.name
    for m in all_modules:
        if m.name not in enabled:
            continue
        for inp in m.inputs:
            src = producer.get(inp)
            if src is not None and src not in enabled:
                raise ValueError(
                    f"Module '{m.name}' needs input '{inp}' produced by disabled "
                    f"module '{src}'. Enable '{src}' or also disable '{m.name}'."
                )

    return [m for m in all_modules if m.name in enabled]
