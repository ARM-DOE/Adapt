# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Core vs extension table classification.

Core tables are produced by the main pipeline (scans, tracks, cells, lineage)
and the catalog itself. Extension tables are produced by post-processors
(e.g. ``lma_cell_stats``). Post-processors may only create extension tables;
the persistence layer refuses to let any module writer target a core table, so
reruns can never corrupt the authoritative pipeline output.
"""

# Authoritative catalog/pipeline tables. Mirrors radar_catalog_schema.sql plus
# the tracking tables written by TrackStore and the module-schema registry.
CORE_TABLES: frozenset[str] = frozenset(
    {
        "items",
        "progress",
        "schemas",
        "module_schemas",
        "scans",
        "cells_by_scan",
        "cell_events",
        "cell_tracks",
    }
)


def is_core_table(name: str) -> bool:
    """Return True if ``name`` is a protected core table."""
    return name in CORE_TABLES
