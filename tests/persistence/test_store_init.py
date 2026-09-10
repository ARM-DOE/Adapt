# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Store initialization: `adapt init` creates the exact layout, once."""

import sqlite3

import pytest

from adapt.persistence.store import AlreadyInitializedError, Store, StoreError, init_store

pytestmark = pytest.mark.unit


class TestInitStore:
    def test_creates_exact_root_layout(self, tmp_path):
        root = tmp_path / "store"

        init_store(root)

        assert {p.name for p in root.iterdir()} == {"registry.db", "logs", "collections"}
        assert (root / "logs").is_dir()
        assert (root / "collections").is_dir()

    def test_registry_has_exact_lifecycle_tables(self, tmp_path):
        root = tmp_path / "store"

        init_store(root)

        conn = sqlite3.connect(root / "registry.db")
        try:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        finally:
            conn.close()
        assert {r[0] for r in rows} == {"collections", "runs", "run_modules", "run_events"}

    def test_init_twice_raises(self, tmp_path):
        root = tmp_path / "store"
        init_store(root)

        with pytest.raises(AlreadyInitializedError, match="already initialized"):
            init_store(root)

    def test_init_on_legacy_root_raises_obsolete_layout(self, tmp_path):
        root = tmp_path / "store"
        root.mkdir()
        (root / "adapt_registry.db").touch()

        with pytest.raises(StoreError, match="obsolete"):
            init_store(root)


class TestStoreOpen:
    def test_open_initialized_root(self, tmp_path):
        root = init_store(tmp_path / "store")

        store = Store.open(root)

        assert store.root == root

    def test_open_uninitialized_root_names_adapt_init(self, tmp_path):
        with pytest.raises(StoreError, match="adapt init"):
            Store.open(tmp_path)

    def test_open_legacy_root_raises_obsolete_layout(self, tmp_path):
        (tmp_path / "adapt_registry.db").touch()

        with pytest.raises(StoreError, match="obsolete"):
            Store.open(tmp_path)
