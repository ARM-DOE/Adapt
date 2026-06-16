# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Behavioral tests for _ensure_modules_registered.

configuration/defaults.yaml is the single source of truth for the core
pipeline module list. Registration must fail loudly when that list is
unreadable, empty, or names a module that cannot be imported (no
fallbacks), and must be idempotent because several components call it
during startup.
"""

import importlib
import inspect

import pytest
import yaml

from adapt.execution import pipeline_builder
from adapt.execution.module_registry import registry
from adapt.execution.pipeline_builder import _DEFAULTS_YAML, _ensure_modules_registered
from adapt.modules.base import BaseModule

pytestmark = pytest.mark.unit


def _declared_module_paths() -> list[str]:
    with open(_DEFAULTS_YAML) as f:
        return yaml.safe_load(f)["pipeline"]["modules"]


def test_every_declared_module_is_registered():
    """Each path in defaults.yaml registers its BaseModule subclass by name."""
    _ensure_modules_registered()

    for path in _declared_module_paths():
        mod = importlib.import_module(path)
        module_classes = [
            cls
            for _, cls in inspect.getmembers(mod, inspect.isclass)
            if issubclass(cls, BaseModule) and cls is not BaseModule and cls.__module__ == path
        ]
        assert module_classes, f"{path} defines no BaseModule subclass"
        for cls in module_classes:
            assert cls.name in registry, (
                f"{path} defines '{cls.name}' but it is not in the registry — "
                "import-time registration is broken"
            )


def test_registration_is_idempotent():
    """Repeated startup calls must not duplicate, reorder, or raise."""
    _ensure_modules_registered()
    before = registry.list_modules()

    _ensure_modules_registered()

    assert registry.list_modules() == before


def test_unreadable_defaults_yaml_raises(monkeypatch, tmp_path):
    """A missing module list aborts startup — there is no fallback pipeline."""
    monkeypatch.setattr(pipeline_builder, "_DEFAULTS_YAML", tmp_path / "missing.yaml")

    with pytest.raises(RuntimeError, match="Cannot read pipeline module list"):
        _ensure_modules_registered()


def test_empty_module_list_raises(monkeypatch, tmp_path):
    """A defaults.yaml that declares no modules is a configuration error."""
    empty = tmp_path / "defaults.yaml"
    empty.write_text("pipeline:\n  modules: []\n")
    monkeypatch.setattr(pipeline_builder, "_DEFAULTS_YAML", empty)

    with pytest.raises(RuntimeError, match="No pipeline modules declared"):
        _ensure_modules_registered()


def test_unimportable_core_module_raises(monkeypatch, tmp_path):
    """A declared module that fails to import aborts startup loudly."""
    bad = tmp_path / "defaults.yaml"
    bad.write_text("pipeline:\n  modules:\n    - adapt.no_such_module\n")
    monkeypatch.setattr(pipeline_builder, "_DEFAULTS_YAML", bad)

    with pytest.raises(ImportError, match="adapt.no_such_module"):
        _ensure_modules_registered()


def test_unimportable_extension_raises():
    """A user extension that fails to import aborts startup loudly."""
    with pytest.raises(ImportError, match="no.such.extension"):
        _ensure_modules_registered(extensions=["no.such.extension"])
