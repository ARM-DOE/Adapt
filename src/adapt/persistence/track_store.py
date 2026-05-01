# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""TrackStore — read/write the three track persistence tables in catalog.db.

Tables managed:
- cells_by_scan : one row per active tracked cell per scan (wide canonical table)
- cell_events   : authoritative lineage edges (CONTINUE/SPLIT/MERGE/INITIATION/TERMINATION)
- cell_tracks   : convenience lifecycle summary per cell_uid

A "track" is a single connected chain of cell observations across scans identified by
a stable cell_uid.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

__all__ = ["TrackStore"]

logger = logging.getLogger(__name__)

_FIXED_CBS_COLS = {
    "run_id", "scan_time", "cell_label", "cell_uid",
    "age_seconds",
    "cell_area_sqkm", "cell_centroid_mass_lat", "cell_centroid_mass_lon",
    "cell_centroid_geom_x", "cell_centroid_geom_y",
    "radar_reflectivity_max", "radar_reflectivity_mean",
    "radar_differential_reflectivity_max", "area_40dbz_km2",
    "n_adjacent_cells", "adjacent_cell_uids_json",
    "is_initiated_here", "is_split_target_here", "is_merge_target_here",
    "is_split_source_here", "is_merge_source_here", "is_terminated_after_here",
}

_SKIP_FROM_CELL_STATS = {
    # tracked internally by tracked_cells with different names; avoid duplicate writes
    "time", "time_volume_start",
}


def _uid_col(df: pd.DataFrame) -> str:
    if "cell_uid" in df.columns:
        return "cell_uid"
    raise ValueError("Missing persistent ID column: expected 'cell_uid'")


def _source_uid(ev: pd.Series):
    return ev.get("source_cell_uid")


def _target_uid(ev: pd.Series):
    return ev.get("target_cell_uid")


class TrackStore:
    """Read/write track persistence tables in catalog.db.

    Thread-safe via SQLite WAL mode and an internal lock.
    Opens its own connection to the same catalog.db used by RadarCatalog.
    """

    def __init__(self, db_path: Path):
        self._db_path = Path(db_path)
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
                isolation_level="DEFERRED",
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._assert_schema(self._conn)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write_scan(
        self,
        run_id: str,
        scan_time: datetime,
        cell_stats_df: pd.DataFrame,
        tracked_cells_df: pd.DataFrame,
        cell_events_df: pd.DataFrame,
        cell_adjacency_df: pd.DataFrame,
    ) -> None:
        """Persist one scan's track outputs to the three tables.

        Parameters
        ----------
        run_id           : pipeline run identifier
        scan_time        : UTC datetime of this scan
        cell_stats_df    : full analysis output (all cell_stats columns)
        tracked_cells_df : tracking module output (cell_uid, cell_label)
        cell_events_df   : tracking module output (lineage events)
        cell_adjacency_df: analysis module output (label-space adjacency)
        """
        if tracked_cells_df.empty:
            return
        if cell_adjacency_df is None:
            raise ValueError("cell_adjacency_df is required (no fallback)")
        if not isinstance(cell_adjacency_df, pd.DataFrame):
            raise TypeError(f"cell_adjacency_df must be a DataFrame, got {type(cell_adjacency_df)}")

        scan_iso = _to_iso(scan_time)
        conn = self._connect()

        with self._lock:
            # 1. Ensure all cell_stats columns exist in cells_by_scan
            self._ensure_columns(conn, cell_stats_df)

            # 1b. Fetch first_seen_time for all active tracks (age computation)
            uid_col = _uid_col(tracked_cells_df)
            cell_uids = tracked_cells_df[uid_col].astype(str).unique().tolist()
            placeholders = ",".join("?" * len(cell_uids))
            first_seen_rows = conn.execute(
                f"SELECT cell_uid, first_seen_time FROM cell_tracks WHERE run_id=? AND cell_uid IN ({placeholders})",
                [run_id] + cell_uids,
            ).fetchall()
            first_seen_map: dict[str, str] = {r["cell_uid"]: r["first_seen_time"] for r in first_seen_rows}

            adjacency = self._build_uid_adjacency_summary(
                tracked_cells_df=tracked_cells_df,
                cell_adjacency_df=cell_adjacency_df,
            )

            # 2. Build cells_by_scan rows
            rows = self._build_cells_rows(
                run_id,
                scan_iso,
                cell_stats_df,
                tracked_cells_df,
                cell_events_df,
                adjacency,
                first_seen_map,
            )

            # 3. Upsert cells_by_scan
            self._upsert_cells(conn, rows)

            # 4. Retroactively update previous scan's cells_by_scan flags
            prev_iso = self._prev_scan_time(conn, run_id, scan_iso)
            if prev_iso and not cell_events_df.empty:
                self._update_retroactive_flags(conn, run_id, prev_iso, cell_events_df)

            # 5. Insert cell_events
            if not cell_events_df.empty:
                self._insert_cell_events(conn, run_id, scan_iso, prev_iso, cell_events_df)

            # 6. Upsert cell_tracks summary
            self._upsert_cell_tracks(conn, run_id, scan_iso, tracked_cells_df, cell_events_df)

            conn.commit()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_cells_by_scan(self, run_id: str, scan_time: datetime) -> pd.DataFrame:
        scan_iso = _to_iso(scan_time)
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM cells_by_scan WHERE run_id=? AND scan_time=?",
                (run_id, scan_iso),
            ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    def get_track_history(self, run_id: str, cell_uid: str) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM cells_by_scan WHERE run_id=? AND cell_uid=? ORDER BY scan_time",
                (run_id, cell_uid),
            ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    def get_cell_events(self, run_id: str, cell_uid: str | None = None) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            if cell_uid is None:
                rows = conn.execute(
                    "SELECT * FROM cell_events WHERE run_id=? ORDER BY event_id",
                    (run_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM cell_events WHERE run_id=? AND (source_cell_uid=? OR target_cell_uid=?) ORDER BY event_id",
                    (run_id, cell_uid, cell_uid),
                ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    def get_cell_tracks(self, run_id: str) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM cell_tracks WHERE run_id=? ORDER BY first_seen_time",
                (run_id,),
            ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _assert_schema(self, conn: sqlite3.Connection) -> None:
        """Fail-fast check for expected (post-rename) schema.

        This codebase intentionally does not attempt to migrate older schemas.
        If a legacy schema is detected, instruct the user to recreate catalog.db.
        """
        tables = {
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        legacy = sorted(t for t in ("tracks", "track_events") if t in tables)
        if legacy:
            raise RuntimeError(
                f"Legacy tracking schema detected (tables={legacy}). "
                "Recreate catalog.db (delete it and rerun the pipeline)."
            )

        required = {"cells_by_scan", "cell_events", "cell_tracks"}
        missing = sorted(required - tables)
        if missing:
            raise RuntimeError(
                f"Missing required tracking tables {missing}. "
                "Ensure catalog.db was created with the current schema."
            )

        cbs_cols = {r[1] for r in conn.execute("PRAGMA table_info(cells_by_scan)").fetchall()}
        if "n_adjacent_tracks" in cbs_cols or any(c.endswith("_index") for c in cbs_cols):
            raise RuntimeError(
                "Legacy index/adjacency columns detected in cells_by_scan. "
                "Recreate catalog.db (delete it and rerun the pipeline)."
            )
        if "n_adjacent_cells" not in cbs_cols:
            raise RuntimeError(
                "cells_by_scan schema mismatch (missing n_adjacent_cells). "
                "Ensure catalog.db was created with the current schema."
            )

        ct_cols = {r[1] for r in conn.execute("PRAGMA table_info(cell_tracks)").fetchall()}
        if any(c.endswith("_index") for c in ct_cols):
            raise RuntimeError(
                "Legacy index columns detected in cell_tracks. "
                "Recreate catalog.db (delete it and rerun the pipeline)."
            )

        ce_cols = {r[1] for r in conn.execute("PRAGMA table_info(cell_events)").fetchall()}
        if any(c.endswith("_index") for c in ce_cols):
            raise RuntimeError(
                "Legacy index columns detected in cell_events. "
                "Recreate catalog.db (delete it and rerun the pipeline)."
            )

    def _build_uid_adjacency_summary(
        self,
        tracked_cells_df: pd.DataFrame,
        cell_adjacency_df: pd.DataFrame,
    ) -> dict[str, tuple[int, str]]:
        """Translate label-space adjacency to per-cell_uid adjacency summary.

        Returns mapping: cell_uid -> (n_adjacent_cells, adjacent_cell_uids_json).
        """
        import json

        if tracked_cells_df.empty:
            raise ValueError("tracked_cells_df is empty (cannot build adjacency summary)")

        required = {"cell_label_a", "cell_label_b", "touching_boundary_pixels"}
        missing = sorted(required - set(cell_adjacency_df.columns))
        if missing:
            raise ValueError(f"cell_adjacency_df missing required columns: {missing}")

        label_to_uid: dict[int, str] = {}
        for _, r in tracked_cells_df.iterrows():
            lbl = int(r["cell_label"])
            uid = str(r["cell_uid"])
            if lbl in label_to_uid and label_to_uid[lbl] != uid:
                raise ValueError(f"Non-unique mapping for cell_label={lbl}: {label_to_uid[lbl]} vs {uid}")
            label_to_uid[lbl] = uid

        neighbors: dict[str, set[str]] = {uid: set() for uid in label_to_uid.values()}
        for _, row in cell_adjacency_df.iterrows():
            a = int(row["cell_label_a"])
            b = int(row["cell_label_b"])
            if a == b:
                raise ValueError("cell_adjacency_df contains a self-pair")
            if a not in label_to_uid or b not in label_to_uid:
                raise ValueError(f"cell_adjacency_df references unknown cell labels: {a}, {b}")
            ua = label_to_uid[a]
            ub = label_to_uid[b]
            neighbors.setdefault(ua, set()).add(ub)
            neighbors.setdefault(ub, set()).add(ua)

        out: dict[str, tuple[int, str]] = {}
        for uid, adj in neighbors.items():
            ids = sorted(adj)
            out[uid] = (len(ids), json.dumps(ids))
        return out

    def _ensure_columns(self, conn: sqlite3.Connection, cell_stats_df: pd.DataFrame) -> None:
        existing = {r[1] for r in conn.execute("PRAGMA table_info(cells_by_scan)").fetchall()}
        for col in cell_stats_df.columns:
            if col in _SKIP_FROM_CELL_STATS or col in _FIXED_CBS_COLS or col in existing:
                continue
            sql_type = _infer_sql_type(col)
            try:
                conn.execute(f"ALTER TABLE cells_by_scan ADD COLUMN {col} {sql_type}")
                #logger.info("cells_by_scan: added column %s %s", col, sql_type)
            except sqlite3.OperationalError:
                pass  # race — column added concurrently

    def _build_cells_rows(
        self,
        run_id: str,
        scan_iso: str,
        cell_stats_df: pd.DataFrame,
        tracked_cells_df: pd.DataFrame,
        cell_events_df: pd.DataFrame,
        adjacency: dict[str, tuple[int, str]],
        first_seen_map: dict[str, str] | None = None,
    ) -> list[dict]:
        # Index cell_stats by cell_label for O(1) lookup
        stats_map = {int(r["cell_label"]): r for _, r in cell_stats_df.iterrows()}

        # Forward flags from current scan events
        initiated = set()
        split_targets = set()
        merge_targets = set()
        if not cell_events_df.empty:
            for _, ev in cell_events_df.iterrows():
                etype = ev["event_type"]
                tcl = ev.get("target_cell_label")
                if etype == "INITIATION" and pd.notna(tcl):
                    initiated.add(int(tcl))
                elif etype == "SPLIT" and pd.notna(tcl):
                    split_targets.add(int(tcl))
                elif etype == "MERGE" and pd.notna(tcl):
                    merge_targets.add(int(tcl))

        # Parse current scan time once for age computation
        from datetime import datetime as _dt
        try:
            scan_dt = _dt.strptime(scan_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        except ValueError:
            scan_dt = None

        rows = []
        uid_col = _uid_col(tracked_cells_df)
        for _, tc in tracked_cells_df.iterrows():
            cl = int(tc["cell_label"])
            tid = str(tc[uid_col])

            # Compute age_seconds from first_seen_time (0 for new initiations)
            age_seconds = 0.0
            if scan_dt is not None and cl not in initiated and first_seen_map and tid in first_seen_map:
                try:
                    first_dt = _dt.strptime(first_seen_map[tid], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
                    age_seconds = max(0.0, (scan_dt - first_dt).total_seconds())
                except ValueError:
                    pass

            row: dict = {
                "run_id": run_id,
                "scan_time": scan_iso,
                "cell_label": cl,
                "cell_uid": tid,
                "age_seconds": age_seconds,
                "n_adjacent_cells": int(adjacency.get(tid, (0, "[]"))[0]),
                "adjacent_cell_uids_json": adjacency.get(tid, (0, "[]"))[1],
                "is_initiated_here": int(cl in initiated),
                "is_split_target_here": int(cl in split_targets),
                "is_merge_target_here": int(cl in merge_targets),
                "is_split_source_here": 0,
                "is_merge_source_here": 0,
                "is_terminated_after_here": 0,
            }
            # Merge all cell_stats columns
            if cl in stats_map:
                for col, val in stats_map[cl].items():
                    if col in _SKIP_FROM_CELL_STATS or col == "cell_label":
                        continue
                    row.setdefault(col, None if pd.isna(val) else val)
            rows.append(row)
        return rows

    def _upsert_cells(self, conn: sqlite3.Connection, rows: list[dict]) -> None:
        if not rows:
            return
        cols = list(rows[0].keys())
        placeholders = ", ".join("?" * len(cols))
        col_list = ", ".join(cols)
        update_set = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in ("run_id", "scan_time", "cell_uid"))
        sql = (
            f"INSERT INTO cells_by_scan ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(run_id, scan_time, cell_uid) DO UPDATE SET {update_set}"
        )
        conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows])

    def _prev_scan_time(self, conn: sqlite3.Connection, run_id: str, scan_iso: str) -> str | None:
        row = conn.execute(
            "SELECT MAX(scan_time) AS t FROM cells_by_scan WHERE run_id=? AND scan_time<?",
            (run_id, scan_iso),
        ).fetchone()
        return row["t"] if row and row["t"] else None

    def _update_retroactive_flags(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        prev_iso: str,
        cell_events_df: pd.DataFrame,
    ) -> None:
        """Set is_split_source, is_merge_source, is_terminated_after on prev scan rows."""
        term_tracks, split_tracks, merge_tracks = set(), set(), set()
        for _, ev in cell_events_df.iterrows():
            etype = ev["event_type"]
            stid = _source_uid(ev)
            if pd.isna(stid):
                continue
            if etype == "TERMINATION":
                term_tracks.add(str(stid))
            elif etype == "SPLIT":
                split_tracks.add(str(stid))
            elif etype == "MERGE":
                merge_tracks.add(str(stid))

        def _update(flag: str, cell_uids: set) -> None:
            for tid in cell_uids:
                conn.execute(
                    f"UPDATE cells_by_scan SET {flag}=1 WHERE run_id=? AND scan_time=? AND cell_uid=?",
                    (run_id, prev_iso, tid),
                )

        _update("is_terminated_after_here", term_tracks)
        _update("is_split_source_here", split_tracks)
        _update("is_merge_source_here", merge_tracks)

    def _insert_cell_events(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        target_iso: str,
        source_iso: str | None,
        cell_events_df: pd.DataFrame,
    ) -> None:
        cols = [
            "run_id", "source_scan_time", "target_scan_time", "event_type",
            "source_cell_uid", "target_cell_uid",
            "source_cell_label", "target_cell_label",
            "cost", "is_dominant", "event_group_id",
        ]
        placeholders = ", ".join("?" * len(cols))
        sql = f"INSERT INTO cell_events ({', '.join(cols)}) VALUES ({placeholders})"

        def _src_time(etype: str) -> str | None:
            return None if etype == "INITIATION" else source_iso

        def _tgt_time(etype: str) -> str | None:
            return None if etype == "TERMINATION" else target_iso

        rows = []
        for _, ev in cell_events_df.iterrows():
            etype = str(ev["event_type"])
            source_uid = _source_uid(ev)
            target_uid = _target_uid(ev)
            rows.append((
                run_id,
                _src_time(etype),
                _tgt_time(etype),
                etype,
                source_uid if pd.notna(source_uid) else None,
                target_uid if pd.notna(target_uid) else None,
                int(ev["source_cell_label"]) if pd.notna(ev.get("source_cell_label")) else None,
                int(ev["target_cell_label"]) if pd.notna(ev.get("target_cell_label")) else None,
                float(ev["cost"]) if pd.notna(ev.get("cost")) else None,
                int(bool(ev.get("is_dominant", False))),
                str(ev["event_group_id"]),
            ))
        conn.executemany(sql, rows)

    def _upsert_cell_tracks(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        scan_iso: str,
        tracked_cells_df: pd.DataFrame,
        cell_events_df: pd.DataFrame,
    ) -> None:
        # Build lookup: cell_uid → (max_area, max_refl)
        active: dict[str, dict] = {}
        uid_col = _uid_col(tracked_cells_df)
        for _, tc in tracked_cells_df.iterrows():
            tid = str(tc[uid_col])
            active[tid] = {
                "area": float(tc.get("area", 0) or 0),
                "refl": float(tc.get("max_reflectivity", 0) or 0),
            }

        # Classify events for origin/termination
        initiated: dict[str, str] = {}    # cell_uid → event_group_id
        split_children: dict[str, tuple[str, str]] = {}  # child_tid → (parent_tid, group_id)
        terminated: dict[str, str] = {}   # cell_uid → event_group_id
        merged_into: dict[str, tuple[str, str]] = {}  # src_tid → (tgt_tid, group_id)

        if not cell_events_df.empty:
            for _, ev in cell_events_df.iterrows():
                etype = str(ev["event_type"])
                gid = str(ev["event_group_id"])
                stid = _source_uid(ev)
                ttid = _target_uid(ev)
                if etype == "INITIATION" and pd.notna(ttid):
                    initiated[str(ttid)] = gid
                elif etype == "SPLIT" and pd.notna(ttid) and pd.notna(stid):
                    split_children[str(ttid)] = (str(stid), gid)
                elif etype == "TERMINATION" and pd.notna(stid):
                    terminated[str(stid)] = gid
                elif etype == "MERGE" and pd.notna(stid) and pd.notna(ttid):
                    merged_into[str(stid)] = (str(ttid), gid)

        # Existing tracks in DB
        existing = {
            r["cell_uid"]: dict(r)
            for r in conn.execute(
                "SELECT cell_uid, n_scans, max_area_sqkm, max_reflectivity FROM cell_tracks WHERE run_id=?",
                (run_id,),
            ).fetchall()
        }

        for tid, info in active.items():
            if tid in existing:
                conn.execute(
                    """UPDATE cell_tracks SET
                        last_seen_time=?,
                        n_scans=n_scans+1,
                        max_area_sqkm=MAX(COALESCE(max_area_sqkm,0), ?),
                        max_reflectivity=MAX(COALESCE(max_reflectivity,0), ?)
                    WHERE run_id=? AND cell_uid=?""",
                    (scan_iso, info["area"], info["refl"], run_id, tid),
                )
            else:
                # Determine origin
                if tid in initiated:
                    origin_type = "INITIATION"
                    origin_grp = initiated[tid]
                    origin_n = 0
                    origin_parent = None
                elif tid in split_children:
                    origin_type = "SPLIT"
                    origin_grp = split_children[tid][1]
                    origin_n = 1
                    origin_parent = split_children[tid][0]
                else:
                    origin_type = "UNKNOWN"
                    origin_grp = None
                    origin_n = 0
                    origin_parent = None

                conn.execute(
                    """INSERT INTO cell_tracks
                        (run_id, cell_uid, first_seen_time, last_seen_time,
                         n_scans, origin_type, origin_event_group_id, origin_n_parents,
                         origin_primary_parent_cell_uid, termination_type,
                         max_area_sqkm, max_reflectivity)
                    VALUES (?,?,?, ?,1,?,?,?,?,'ACTIVE_AT_END',?,?)
                    ON CONFLICT(run_id, cell_uid) DO UPDATE SET
                        last_seen_time=excluded.last_seen_time,
                        n_scans=cell_tracks.n_scans+1,
                        max_area_sqkm=MAX(COALESCE(cell_tracks.max_area_sqkm,0), excluded.max_area_sqkm),
                        max_reflectivity=MAX(COALESCE(cell_tracks.max_reflectivity,0), excluded.max_reflectivity)""",
                    (run_id, tid, scan_iso, scan_iso,
                     origin_type, origin_grp, origin_n, origin_parent,
                     info["area"], info["refl"]),
                )

        # Update termination for tracks not in this scan
        for tid, gid in terminated.items():
            if tid not in active:
                if tid in merged_into:
                    tgt_tid, merge_gid = merged_into[tid]
                    conn.execute(
                        """UPDATE cell_tracks SET termination_type='MERGED',
                            termination_event_group_id=?, terminated_into_cell_uid=?
                        WHERE run_id=? AND cell_uid=?""",
                        (merge_gid, tgt_tid, run_id, tid),
                    )
                else:
                    conn.execute(
                        """UPDATE cell_tracks SET termination_type='TERMINATION',
                            termination_event_group_id=?
                        WHERE run_id=? AND cell_uid=?""",
                        (gid, run_id, tid),
                    )


# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------

def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _infer_sql_type(col: str) -> str:
    col_l = col.lower()
    if any(col_l.endswith(s) for s in ("_lat", "_lon", "_mean", "_max", "_min", "_sqkm", "_km2", "_std", "_p25", "_p75")):
        return "REAL"
    if any(col_l.endswith(s) for s in ("_x", "_y", "_count", "_pixels", "_index")):
        return "INTEGER"
    if col_l.startswith("radar_"):
        return "REAL"
    return "TEXT"
