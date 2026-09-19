# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Deterministic identity for raw source data.

``scan_id`` is the single identity key joining tracking tables, catalog rows,
artifacts, and consumer lookups (design/08_repository_evolution.md: "stable scan
identity and UTC observation time"). Identity derives from the raw file bytes —
never from a filename, clock, or run — so the same volume always receives the
same id across runs, hosts, and re-downloads.

Pure function of its input: no I/O, no state. Callers read the file bytes.
"""

import hashlib

# 16 hex chars of sha256: matches the items.item_id convention and is externally
# verifiable with standard tooling (`sha256sum <raw file> | cut -c1-16`).
_SCAN_ID_HEX_CHARS = 16


def scan_id_from_bytes(data: bytes) -> str:
    """Mint the scan_id for a raw source file from its exact bytes."""
    if not isinstance(data, bytes):
        raise TypeError(f"scan_id_from_bytes expects bytes, got {type(data).__name__}")
    return hashlib.sha256(data).hexdigest()[:_SCAN_ID_HEX_CHARS]
