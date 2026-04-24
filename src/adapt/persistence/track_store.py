"""TrackStore — read/write the three track persistence tables in catalog.db.

Tables managed:
- cells_by_scan : one row per active tracked cell per scan (wide canonical table)
- track_events  : authoritative lineage edges (CONTINUE/SPLIT/MERGE/INITIATION/TERMINATION)
- tracks        : convenience lifecycle summary per cell_uid

A "track" is a single connected chain of cell observations across scans identified by
a stable cell_uid.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

__all__ = ["TrackStore"]

logger = logging.getLogger(__name__)

_FIXED_CBS_COLS = {
    "run_id", "scan_time", "cell_label", "cell_uid", "track_index",
    "age_seconds",
    "cell_area_sqkm", "cell_centroid_mass_lat", "cell_centroid_mass_lon",
    "cell_centroid_geom_x", "cell_centroid_geom_y",
    "radar_reflectivity_max", "radar_reflectivity_mean",
    "radar_differential_reflectivity_max", "area_40dbz_km2",
    "n_adjacent_tracks", "adjacent_cell_uids_json",
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
        self._conn: Optional[sqlite3.Connection] = None

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
        track_events_df: pd.DataFrame,
        track_adj_df: pd.DataFrame,
    ) -> None:
        """Persist one scan's track outputs to the three tables.

        Parameters
        ----------
        run_id           : pipeline run identifier
        scan_time        : UTC datetime of this scan
        cell_stats_df    : full analysis output (all cell_stats columns)
        tracked_cells_df : tracking module output (cell_uid, track_index, adjacency)
        track_events_df  : tracking module output (lineage events)
        track_adj_df     : tracking module output (track adjacency)
        """
        if tracked_cells_df.empty:
            return

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
                f"SELECT cell_uid, first_seen_time FROM tracks WHERE run_id=? AND cell_uid IN ({placeholders})",
                [run_id] + cell_uids,
            ).fetchall()
            first_seen_map: dict[str, str] = {r["cell_uid"]: r["first_seen_time"] for r in first_seen_rows}

            # 2. Build cells_by_scan rows
            rows = self._build_cells_rows(
                run_id, scan_iso, cell_stats_df, tracked_cells_df, track_events_df, first_seen_map
            )

            # 3. Upsert cells_by_scan
            self._upsert_cells(conn, rows)

            # 4. Retroactively update previous scan's cells_by_scan flags
            prev_iso = self._prev_scan_time(conn, run_id, scan_iso)
            if prev_iso and not track_events_df.empty:
                self._update_retroactive_flags(conn, run_id, prev_iso, track_events_df)

            # 5. Insert track_events
            if not track_events_df.empty:
                self._insert_track_events(conn, run_id, scan_iso, prev_iso, track_events_df)

            # 6. Upsert tracks summary
            self._upsert_tracks(conn, run_id, scan_iso, tracked_cells_df, track_events_df)

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

    def get_track_events(self, run_id: str, cell_uid: Optional[str] = None) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            if cell_uid is None:
                rows = conn.execute(
                    "SELECT * FROM track_events WHERE run_id=? ORDER BY event_id",
                    (run_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM track_events WHERE run_id=? AND (source_cell_uid=? OR target_cell_uid=?) ORDER BY event_id",
                    (run_id, cell_uid, cell_uid),
                ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    def get_tracks(self, run_id: str) -> pd.DataFrame:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM tracks WHERE run_id=? ORDER BY track_index",
                (run_id,),
            ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_columns(self, conn: sqlite3.Connection, cell_stats_df: pd.DataFrame) -> None:
        existing = {r[1] for r in conn.execute("PRAGMA table_info(cells_by_scan)").fetchall()}
        for col in cell_stats_df.columns:
            if col in _SKIP_FROM_CELL_STATS or col in _FIXED_CBS_COLS or col in existing:
                continue
            sql_type = _infer_sql_type(col)
            try:
                conn.execute(f"ALTER TABLE cells_by_scan ADD COLUMN {col} {sql_type}")
                logger.info("cells_by_scan: added column %s %s", col, sql_type)
            except sqlite3.OperationalError:
                pass  # race — column added concurrently

    def _build_cells_rows(
        self,
        run_id: str,
        scan_iso: str,
        cell_stats_df: pd.DataFrame,
        tracked_cells_df: pd.DataFrame,
        track_events_df: pd.DataFrame,
        first_seen_map: dict[str, str] | None = None,
    ) -> list[dict]:
        # Index cell_stats by cell_label for O(1) lookup
        stats_map = {int(r["cell_label"]): r for _, r in cell_stats_df.iterrows()}

        # Forward flags from current scan events
        initiated = set()
        split_targets = set()
        merge_targets = set()
        if not track_events_df.empty:
            for _, ev in track_events_df.iterrows():
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
            scan_dt = _dt.strptime(scan_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
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
                    first_dt = _dt.strptime(first_seen_map[tid], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    age_seconds = max(0.0, (scan_dt - first_dt).total_seconds())
                except ValueError:
                    pass

            row: dict = {
                "run_id": run_id,
                "scan_time": scan_iso,
                "cell_label": cl,
                "cell_uid": tid,
                "track_index": int(tc["track_index"]),
                "age_seconds": age_seconds,
                "n_adjacent_tracks": int(tc.get("n_adjacent_tracks", 0) or 0),
                "adjacent_cell_uids_json": tc.get("adjacent_cell_uids_json"),
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

    def _prev_scan_time(self, conn: sqlite3.Connection, run_id: str, scan_iso: str) -> Optional[str]:
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
        track_events_df: pd.DataFrame,
    ) -> None:
        """Set is_split_source, is_merge_source, is_terminated_after on prev scan rows."""
        term_tracks, split_tracks, merge_tracks = set(), set(), set()
        for _, ev in track_events_df.iterrows():
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

    def _insert_track_events(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        target_iso: str,
        source_iso: Optional[str],
        track_events_df: pd.DataFrame,
    ) -> None:
        cols = [
            "run_id", "source_scan_time", "target_scan_time", "event_type",
            "source_cell_uid", "target_cell_uid",
            "source_track_index", "target_track_index",
            "source_cell_label", "target_cell_label",
            "cost", "is_dominant", "event_group_id",
        ]
        placeholders = ", ".join("?" * len(cols))
        sql = f"INSERT INTO track_events ({', '.join(cols)}) VALUES ({placeholders})"

        def _src_time(etype: str) -> Optional[str]:
            return None if etype == "INITIATION" else source_iso

        def _tgt_time(etype: str) -> Optional[str]:
            return None if etype == "TERMINATION" else target_iso

        rows = []
        for _, ev in track_events_df.iterrows():
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
                int(ev["source_track_index"]) if pd.notna(ev.get("source_track_index")) else None,
                int(ev["target_track_index"]) if pd.notna(ev.get("target_track_index")) else None,
                int(ev["source_cell_label"]) if pd.notna(ev.get("source_cell_label")) else None,
                int(ev["target_cell_label"]) if pd.notna(ev.get("target_cell_label")) else None,
                float(ev["cost"]) if pd.notna(ev.get("cost")) else None,
                int(bool(ev.get("is_dominant", False))),
                str(ev["event_group_id"]),
            ))
        conn.executemany(sql, rows)

    def _upsert_tracks(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        scan_iso: str,
        tracked_cells_df: pd.DataFrame,
        track_events_df: pd.DataFrame,
    ) -> None:
        # Build lookup: cell_uid → (track_index, max_area, max_refl)
        active: dict[str, dict] = {}
        uid_col = _uid_col(tracked_cells_df)
        for _, tc in tracked_cells_df.iterrows():
            tid = str(tc[uid_col])
            active[tid] = {
                "track_index": int(tc["track_index"]),
                "area": float(tc.get("area", 0) or 0),
                "refl": float(tc.get("max_reflectivity", 0) or 0),
            }

        # Classify events for origin/termination
        initiated: dict[str, str] = {}    # cell_uid → event_group_id
        split_children: dict[str, tuple[str, str]] = {}  # child_tid → (parent_tid, group_id)
        terminated: dict[str, str] = {}   # cell_uid → event_group_id
        merged_into: dict[str, tuple[str, str]] = {}  # src_tid → (tgt_tid, group_id)

        if not track_events_df.empty:
            for _, ev in track_events_df.iterrows():
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
                "SELECT cell_uid, n_scans, max_area_sqkm, max_reflectivity FROM tracks WHERE run_id=?",
                (run_id,),
            ).fetchall()
        }

        for tid, info in active.items():
            if tid in existing:
                conn.execute(
                    """UPDATE tracks SET
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
                    """INSERT INTO tracks
                        (run_id, cell_uid, track_index, first_seen_time, last_seen_time,
                         n_scans, origin_type, origin_event_group_id, origin_n_parents,
                         origin_primary_parent_cell_uid, termination_type,
                         max_area_sqkm, max_reflectivity)
                    VALUES (?,?,?,?,?,1,?,?,?,?,'ACTIVE_AT_END',?,?)
                    ON CONFLICT(run_id, cell_uid) DO UPDATE SET
                        last_seen_time=excluded.last_seen_time,
                        n_scans=tracks.n_scans+1,
                        max_area_sqkm=MAX(COALESCE(tracks.max_area_sqkm,0), excluded.max_area_sqkm),
                        max_reflectivity=MAX(COALESCE(tracks.max_reflectivity,0), excluded.max_reflectivity)""",
                    (run_id, tid, info["track_index"], scan_iso, scan_iso,
                     origin_type, origin_grp, origin_n, origin_parent,
                     info["area"], info["refl"]),
                )

        # Update termination for tracks not in this scan
        for tid, gid in terminated.items():
            if tid not in active:
                if tid in merged_into:
                    tgt_tid, merge_gid = merged_into[tid]
                    conn.execute(
                        """UPDATE tracks SET termination_type='MERGED',
                            termination_event_group_id=?, terminated_into_cell_uid=?
                        WHERE run_id=? AND cell_uid=?""",
                        (merge_gid, tgt_tid, run_id, tid),
                    )
                else:
                    conn.execute(
                        """UPDATE tracks SET termination_type='TERMINATION',
                            termination_event_group_id=?
                        WHERE run_id=? AND cell_uid=?""",
                        (gid, run_id, tid),
                    )


# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------

def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
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
