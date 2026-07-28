# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Commented-YAML serializer for generated config files.

Emits a plain config dict as YAML, annotating scalar leaves with their field
descriptions as inline ``# comment`` text. Parsing the result reproduces the
original data (comments and tuple→list normalisation aside). Pure formatting;
stdlib only.
"""

import re
from typing import Any

# Descriptions are a tree mirroring the data: a key maps to either a leaf
# description string or a nested dict of descriptions.
Descriptions = dict[str, Any]

_PLAIN = re.compile(r"^[A-Za-z0-9_./+-]+$")


def dump(data: dict, descriptions: Descriptions | None = None, header: str = "") -> str:
    """Serialize ``data`` to commented YAML.

    Parameters
    ----------
    data : dict
        Config to serialize (scalars, lists/tuples, nested dicts).
    descriptions : dict, optional
        Tree of per-field descriptions mirroring ``data``; scalar leaves with a
        description get an inline ``# comment``.
    header : str, optional
        Pre-formatted comment block placed at the top (caller includes ``#``).
    """
    lines: list[str] = []
    if header:
        lines.append(header.rstrip("\n"))
    _emit(data, descriptions or {}, 0, lines)
    return "\n".join(lines) + "\n"


def _emit(data: dict, desc: Descriptions, indent: int, lines: list[str]) -> None:
    pad = "  " * indent
    for key, value in data.items():
        d = desc.get(key) if isinstance(desc, dict) else None
        if isinstance(value, dict):
            lines.append(f"{pad}{key}:")
            _emit(value, d if isinstance(d, dict) else {}, indent + 1, lines)
        elif isinstance(value, list | tuple) and isinstance(d, dict):
            # A flat list with a dict description → block style with a leading
            # header comment and per-item inline comments (e.g. the modules list).
            _emit_commented_sequence(key, value, d, pad, indent, lines)
        elif isinstance(value, list | tuple):
            _emit_sequence(key, value, pad, indent, lines)
        else:
            comment = f"  # {d}" if isinstance(d, str) and d else ""
            lines.append(f"{pad}{key}: {_scalar(value)}{comment}")


def _emit_sequence(key: str, value, pad: str, indent: int, lines: list[str]) -> None:
    items = list(value)
    if all(not isinstance(i, list | tuple | dict) for i in items):
        # Flat sequence → inline flow style.
        inline = ", ".join(_scalar(i) for i in items)
        lines.append(f"{pad}{key}: [{inline}]")
        return
    # Nested sequence → block style, one inline row per item.
    lines.append(f"{pad}{key}:")
    item_pad = "  " * (indent + 1)
    for item in items:
        inline = ", ".join(_scalar(i) for i in item)
        lines.append(f"{item_pad}- [{inline}]")


def _emit_commented_sequence(
    key: str, value, desc: dict, pad: str, indent: int, lines: list[str]
) -> None:
    """Block-style list with a leading ``_header`` comment and per-item comments."""
    header = desc.get("_header")
    if isinstance(header, str) and header:
        lines.extend(f"{pad}# {hl}" for hl in header.splitlines())
    lines.append(f"{pad}{key}:")
    item_pad = "  " * (indent + 1)
    for item in value:
        c = desc.get(item)
        comment = f"  # {c}" if isinstance(c, str) and c else ""
        lines.append(f"{item_pad}- {_scalar(item)}{comment}")


def _scalar(value: Any) -> str:
    """Render a scalar as a YAML token."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        if _PLAIN.match(value):
            return value
        # Single-quoted, not double-quoted: YAML processes escape sequences inside
        # double quotes, so a backslash would corrupt the value (and "\U" in a
        # Windows path is a hard parse error). In single quotes the only escape is
        # '' for a literal quote, so any path or free text round-trips verbatim.
        return "'" + value.replace("'", "''") + "'"
    return str(value)
