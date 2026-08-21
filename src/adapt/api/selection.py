# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""FilterSpec — API-level, immutable filter over a track population."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

__all__ = ["FilterSpec"]


@dataclass(frozen=True)
class FilterSpec:
    """Immutable filter over a run's track population.

    Each field is optional (None = no constraint). All constraints are ANDed.
    Compiles to SQL WHERE clause via :meth:`to_sql_where`.

    The clause references ``cell_tracks`` columns:
    ``first_seen_time``, ``last_seen_time``, ``n_scans``, ``max_area_sqkm``,
    ``max_reflectivity``, ``origin_type``, ``termination_type``.
    """

    lifetime_min_s: float | None = None
    lifetime_max_s: float | None = None
    n_scans_min: int | None = None
    max_area_min_km2: float | None = None
    max_area_max_km2: float | None = None
    # Filters the cell_tracks.max_reflectivity column: max of the run's
    # TRACKED field (dBZ only for reflectivity-tracked runs).
    max_refl_min_dbz: float | None = None
    max_refl_max_dbz: float | None = None
    origin_types: frozenset[str] | None = None
    termination_types: frozenset[str] | None = None
    required_tags: frozenset[str] | None = None
    first_seen_after: datetime | None = None
    first_seen_before: datetime | None = None

    def is_empty(self) -> bool:
        """Return True when no constraints are set (matches all tracks)."""
        return (
            self.lifetime_min_s is None
            and self.lifetime_max_s is None
            and self.n_scans_min is None
            and self.max_area_min_km2 is None
            and self.max_area_max_km2 is None
            and self.max_refl_min_dbz is None
            and self.max_refl_max_dbz is None
            and self.origin_types is None
            and self.termination_types is None
            and self.required_tags is None
            and self.first_seen_after is None
            and self.first_seen_before is None
        )

    def to_sql_where(self) -> tuple[str, list]:
        """Compile filter to a SQL WHERE clause and positional parameter list.

        Returns
        -------
        clause : str
            SQL fragment starting with ``WHERE`` if any constraints exist,
            empty string otherwise.
        params : list
            Positional parameters matching ``?`` placeholders in *clause*.
        """
        conditions: list[str] = []
        params: list = []

        if self.lifetime_min_s is not None:
            conditions.append(
                "(julianday(last_seen_time) - julianday(first_seen_time)) * 86400 >= ?"
            )
            params.append(self.lifetime_min_s)

        if self.lifetime_max_s is not None:
            conditions.append(
                "(julianday(last_seen_time) - julianday(first_seen_time)) * 86400 <= ?"
            )
            params.append(self.lifetime_max_s)

        if self.n_scans_min is not None:
            conditions.append("n_scans >= ?")
            params.append(self.n_scans_min)

        if self.max_area_min_km2 is not None:
            conditions.append("max_area_sqkm >= ?")
            params.append(self.max_area_min_km2)

        if self.max_area_max_km2 is not None:
            conditions.append("max_area_sqkm <= ?")
            params.append(self.max_area_max_km2)

        if self.max_refl_min_dbz is not None:
            conditions.append("max_reflectivity >= ?")
            params.append(self.max_refl_min_dbz)

        if self.max_refl_max_dbz is not None:
            conditions.append("max_reflectivity <= ?")
            params.append(self.max_refl_max_dbz)

        if self.origin_types is not None:
            placeholders = ", ".join("?" * len(self.origin_types))
            conditions.append(f"origin_type IN ({placeholders})")
            params.extend(sorted(self.origin_types))  # sorted for determinism

        if self.termination_types is not None:
            placeholders = ", ".join("?" * len(self.termination_types))
            conditions.append(f"termination_type IN ({placeholders})")
            params.extend(sorted(self.termination_types))

        if self.first_seen_after is not None:
            conditions.append("first_seen_time >= ?")
            params.append(self.first_seen_after.isoformat())

        if self.first_seen_before is not None:
            conditions.append("first_seen_time <= ?")
            params.append(self.first_seen_before.isoformat())

        clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        return clause, params
