# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Human-readable configuration errors.

Pydantic's raw ``ValidationError`` names the offending path but buries it in a
stack trace and codes. ``validated()`` translates it into a ``ConfigError`` that
states, per problem, which config section/field is wrong, its value, and why —
so a stale or mistyped option reads clearly instead of dumping a traceback.
"""

from collections.abc import Mapping
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

__all__ = ["ConfigError", "format_validation_error", "validated"]

M = TypeVar("M", bound=BaseModel)


class ConfigError(Exception):
    """A configuration value is invalid. Carries a ready-to-print message."""


def _reason(error: Mapping[str, Any]) -> str:
    """One-line 'why' for a single pydantic error, keyed off its type."""
    kind = error["type"]
    value = error.get("input")
    loc = error["loc"]
    section = str(loc[-2]) if len(loc) >= 2 else "configuration"
    if kind == "extra_forbidden":
        return (
            f"unrecognized option (value: {value!r}) — not a valid '{section}' setting; "
            "remove it or check for a typo or a renamed field"
        )
    if kind == "missing":
        return "required option is missing"
    if kind in ("greater_than", "greater_than_equal", "less_than", "less_than_equal"):
        return f"value {value!r} is out of range — {error['msg']}"
    return f"{error['msg']} (value: {value!r})"


def format_validation_error(exc: ValidationError, source: str) -> str:
    """Render a pydantic ValidationError as a clear, multi-line config message."""
    lines = [f"Invalid configuration ({source}):"]
    for error in exc.errors():
        path = ".".join(str(part) for part in error["loc"]) or "(root)"
        lines.append(f"  • {path} — {_reason(error)}")
    return "\n".join(lines)


def validated(model_cls: type[M], data: Any, *, source: str) -> M:
    """Validate ``data`` against ``model_cls``, raising ConfigError on failure.

    ``source`` names where the config came from (a file path, "command-line
    flags", ...) so the message points the user at the right place to fix.
    """
    try:
        return model_cls.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(format_validation_error(exc, source)) from exc
