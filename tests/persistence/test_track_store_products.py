# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""TrackStore on products.db: first-write freeze via SchemaLedger, no ALTER ever."""

import sqlite3
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from adapt.persistence.errors import StoreError
from adapt.persistence.products import SchemaLedger
from adapt.persistence.store import Store, init_store
from adapt.persistence.track_store import TrackStore

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)
RUN = "run-1"


def _stats(labels, extra_cols=None):
    data = {"cell_label": labels, "cell_area_sqkm": [10.0] * len(labels)}
    data.update(extra_cols or {})
    return pd.DataFrame(data)


def _tracked(pairs):
    return pd.DataFrame(
        {
            "cell_label": [label for label, _ in pairs],
            "cell_uid": [uid for _, uid in pairs],
            "area": [10.0] * len(pairs),
            "max_reflectivity": [45.0] * len(pairs),
        }
    )


def _no_events():
    return pd.DataFrame()


def _no_adjacency():
    return pd.DataFrame(columns=["cell_label_a", "cell_label_b", "touching_boundary_pixels"])


@pytest.fixture
def collection(tmp_path):
    root = init_store(tmp_path / "store")
    store = Store.open(root)
    yield store.collection("KILX")
    store.close()


@pytest.fixture
def ledger(collection):
    return SchemaLedger(collection.products_path, collection.catalog)


def _write(collection, ledger, scan_id, offset_s=0, stats=None, tracked=None, events=None):
    with TrackStore(collection.products_path, ledger=ledger) as ts:
        ts.write_scan(
            run_id=RUN,
            scan_time=SCAN_TIME + timedelta(seconds=offset_s),
            cell_stats_df=_stats([1]) if stats is None else stats,
            tracked_cells_df=_tracked([(1, "u1")]) if tracked is None else tracked,
            cell_events_df=_no_events() if events is None else events,
            cell_adjacency_df=_no_adjacency(),
            scan_id=scan_id,
        )


def _table_rows(collection, table, where):
    conn = sqlite3.connect(collection.products_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(f"SELECT * FROM {table} WHERE {where}").fetchall()]
    finally:
        conn.close()


def _table_info(collection, table):
    conn = sqlite3.connect(collection.products_path)
    try:
        return conn.execute(f"PRAGMA table_info({table})").fetchall()
    finally:
        conn.close()


class TestFreezeOnFirstWrite:
    def test_first_write_creates_three_tables_and_snapshots(self, collection, ledger):
        _write(collection, ledger, "s1", stats=_stats([1], {"custom_metric": [3.5]}))

        for table in ("cells_by_scan", "cell_events", "cell_tracks"):
            frozen = ledger.frozen(table)
            assert frozen is not None, table
            assert frozen.owner_module == "tracking"
            assert _table_info(collection, table), table

        cbs_cols = {name for name, _ in ledger.frozen("cells_by_scan").columns}
        assert "custom_metric" in cbs_cols
        assert {"run_id", "scan_id", "scan_time", "cell_uid", "cell_label"} <= cbs_cols

    def test_extra_numeric_columns_freeze_as_real(self, collection, ledger):
        # Source-dependent stats columns follow the deterministic rule: the
        # frozen type is a function of the column name, never of whether the
        # first scan's values happened to be ints or NaN-promoted floats.
        _write(collection, ledger, "s1", stats=_stats([1], {"echo_top_count": [3]}))

        types = dict(ledger.frozen("cells_by_scan").columns)
        assert types["echo_top_count"] == "REAL"

    def test_duplicate_label_within_scan_raises(self, collection, ledger):
        _write(collection, ledger, "s1")

        with pytest.raises(ValueError, match="duplicate"):
            _write(
                collection,
                ledger,
                "s2",
                offset_s=300,
                stats=_stats([1, 1]),
                tracked=_tracked([(1, "u1"), (1, "u2")]),
            )

    def test_new_stats_column_after_freeze_raises(self, collection, ledger):
        _write(collection, ledger, "s1")

        with pytest.raises(StoreError, match="late_arrival"):
            _write(
                collection,
                ledger,
                "s2",
                offset_s=300,
                stats=_stats([1], {"late_arrival": [1.0]}),
            )

    def test_construction_requires_the_ledger(self, collection):
        with pytest.raises(TypeError):
            TrackStore(collection.products_path)  # type: ignore[call-arg]


class TestScience:
    def test_track_continuity_and_age_across_scans(self, collection, ledger):
        _write(collection, ledger, "s1")
        _write(collection, ledger, "s2", offset_s=300)

        with TrackStore(collection.products_path, ledger=ledger) as ts:
            history = ts.get_track_history(RUN, "u1")
            tracks = ts.get_cell_tracks(RUN)
        assert list(history["scan_id"]) == ["s1", "s2"]
        assert history["age_seconds"].tolist() == [0.0, 300.0]
        assert tracks.loc[0, "n_scans"] == 2

    def test_retroactive_split_flag_on_previous_scan(self, collection, ledger):
        _write(collection, ledger, "s1")
        events = pd.DataFrame(
            [
                {
                    "event_type": "SPLIT",
                    "source_cell_uid": "u1",
                    "target_cell_uid": "u2",
                    "source_cell_label": 1,
                    "target_cell_label": 2,
                    "cost": 0.1,
                    "is_dominant": True,
                    "event_group_id": "g1",
                }
            ]
        )
        _write(
            collection,
            ledger,
            "s2",
            offset_s=300,
            stats=_stats([1, 2]),
            tracked=_tracked([(1, "u1"), (2, "u2")]),
            events=events,
        )

        with TrackStore(collection.products_path, ledger=ledger) as ts:
            s1_cells = ts.get_cells_by_scan(RUN, "s1")
            events_df = ts.get_cell_events(RUN, "u1")
        assert s1_cells.loc[0, "is_split_source_here"] == 1
        assert events_df.loc[0, "source_scan_id"] == "s1"
        assert events_df.loc[0, "target_scan_id"] == "s2"


def _event(event_type, source, target=None, source_scan=None, target_label=None):
    return {
        "event_type": event_type,
        "source_cell_uid": source,
        "target_cell_uid": target,
        "source_cell_label": 1,
        "target_cell_label": target_label,
        "source_scan_id": source_scan,
        "source_scan_time": None if source_scan is None else SCAN_TIME,
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{event_type}:{source}",
    }


class TestLatentTracks:
    """A track that vanishes is kept latent; its end or resumption is decided scans
    later and refers back to the scan it was last observed in."""

    def _vanish(self, collection, ledger):
        _write(collection, ledger, "s1")
        _write(
            collection,
            ledger,
            "s2",
            offset_s=300,
            stats=_stats([2]),
            tracked=_tracked([(2, "u2")]),
            events=pd.DataFrame([_event("LATENT", "u1", source_scan="s1")]),
        )

    def test_a_latent_track_is_not_terminated(self, collection, ledger):
        self._vanish(collection, ledger)
        with TrackStore(collection.products_path, ledger=ledger) as ts:
            s1 = ts.get_cells_by_scan(RUN, "s1")
            tracks = ts.get_cell_tracks(RUN).set_index("cell_uid")
        assert s1.loc[0, "is_terminated_after_here"] == 0
        assert tracks.loc["u1", "termination_type"] == "ACTIVE_AT_END"

    def test_expiry_terminates_at_the_last_observed_scan(self, collection, ledger):
        self._vanish(collection, ledger)
        _write(
            collection,
            ledger,
            "s3",
            offset_s=600,
            stats=_stats([2]),
            tracked=_tracked([(2, "u2")]),
            events=pd.DataFrame([_event("TERMINATION", "u1", source_scan="s1")]),
        )
        with TrackStore(collection.products_path, ledger=ledger) as ts:
            s1 = ts.get_cells_by_scan(RUN, "s1")
            events = ts.get_cell_events(RUN, "u1").set_index("event_type")
            tracks = ts.get_cell_tracks(RUN).set_index("cell_uid")
        assert s1.loc[0, "is_terminated_after_here"] == 1
        assert events.loc["TERMINATION", "source_scan_id"] == "s1"
        assert tracks.loc["u1", "termination_type"] == "TERMINATION"

    def test_an_expired_merge_source_ends_merged_into_its_survivor(self, collection, ledger):
        self._vanish(collection, ledger)
        _write(
            collection,
            ledger,
            "s3",
            offset_s=600,
            stats=_stats([2]),
            tracked=_tracked([(2, "u2")]),
            events=pd.DataFrame([_event("TERMINATION", "u1", "u2", source_scan="s1")]),
        )
        with TrackStore(collection.products_path, ledger=ledger) as ts:
            tracks = ts.get_cell_tracks(RUN).set_index("cell_uid")
        assert tracks.loc["u1", "termination_type"] == "MERGED"
        assert tracks.loc["u1", "terminated_into_cell_uid"] == "u2"

    def test_a_resumed_track_continues_its_age(self, collection, ledger):
        self._vanish(collection, ledger)
        _write(
            collection,
            ledger,
            "s3",
            offset_s=600,
            stats=_stats([1, 2]),
            tracked=_tracked([(1, "u1"), (2, "u2")]),
            events=pd.DataFrame([_event("RESUMED", "u1", "u1", source_scan="s1", target_label=1)]),
        )
        with TrackStore(collection.products_path, ledger=ledger) as ts:
            history = ts.get_track_history(RUN, "u1")
            events = ts.get_cell_events(RUN, "u1").set_index("event_type")
        assert history["age_seconds"].tolist() == [0.0, 600.0]
        assert events.loc["RESUMED", "source_scan_id"] == "s1"
        assert events.loc["RESUMED", "target_scan_id"] == "s3"


class TestStrictTrackedColumns:
    def test_missing_max_reflectivity_column_raises(self, collection, ledger):
        # A tracked_cells frame without max_reflectivity must raise loudly,
        # never write 0.0 into cell_tracks (silent corruption).
        bad_tracked = pd.DataFrame({"cell_label": [1], "cell_uid": ["u1"], "area": [10.0]})
        with pytest.raises(KeyError, match="max_reflectivity"):
            _write(collection, ledger, "s1", tracked=bad_tracked)

    def test_missing_area_column_raises(self, collection, ledger):
        bad_tracked = pd.DataFrame(
            {"cell_label": [1], "cell_uid": ["u1"], "max_reflectivity": [45.0]}
        )
        with pytest.raises(KeyError, match="area"):
            _write(collection, ledger, "s1", tracked=bad_tracked)

    def test_cells_with_heterogeneous_stats_columns_all_persist(self, collection, ledger):
        # First write freezes the schema including the extra column.
        _write(collection, ledger, "s1", stats=_stats([1], {"custom_metric": [3.5]}))
        # Second scan (5 min later): two cells, only cell 1 has a stats row
        # (cell 2 is in tracked_cells but absent from cell_stats) -> rows
        # have different key sets. Must not raise; missing values -> NULL.
        stats = _stats([1], {"custom_metric": [4.5]})
        tracked = _tracked([(1, "u1"), (2, "u2")])
        _write(collection, ledger, "s2", offset_s=300, stats=stats, tracked=tracked)

        rows = {
            r["cell_uid"]: r for r in _table_rows(collection, "cells_by_scan", "scan_id = 's2'")
        }
        assert set(rows) == {"u1", "u2"}
        assert rows["u1"]["custom_metric"] == 4.5
        assert rows["u2"]["custom_metric"] is None

    def test_reversed_order_second_cell_carries_extra_stats(self, collection, ledger):
        # Mirror case: the FIRST row lacks the stats columns and a LATER row
        # has them — previously those columns were silently dropped.
        _write(collection, ledger, "s1", stats=_stats([1], {"custom_metric": [3.5]}))
        stats = pd.DataFrame({"cell_label": [2], "cell_area_sqkm": [10.0], "custom_metric": [7.5]})
        tracked = _tracked([(1, "u1"), (2, "u2")])
        _write(collection, ledger, "s2", offset_s=300, stats=stats, tracked=tracked)

        rows = {
            r["cell_uid"]: r for r in _table_rows(collection, "cells_by_scan", "scan_id = 's2'")
        }
        assert rows["u2"]["custom_metric"] == 7.5
        assert rows["u1"]["custom_metric"] is None
