"""Tests for the shared SqliteStore base behaviour."""

import pytest

from adapt.persistence.sqlite_store import SqliteStore

pytestmark = pytest.mark.unit


def test_missing_schema_file_raises(tmp_path):
    """A nonexistent schema file fails loudly — there is no inline fallback."""
    with pytest.raises(FileNotFoundError, match="nonexistent_schema.sql"):
        SqliteStore(tmp_path / "x.db", "nonexistent_schema.sql")


def test_registry_schema_loads_from_file(tmp_path):
    """The real registry schema loads, including the schema_registry table."""
    store = SqliteStore(tmp_path / "reg.db", "registry_schema.sql")
    try:
        tables = {
            row[0]
            for row in store._get_connection().execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        store.close()

    assert {"runs", "radars", "item_types", "schema_registry"} <= tables


def test_close_is_idempotent(tmp_path):
    """close() may be called repeatedly without error."""
    store = SqliteStore(tmp_path / "reg.db", "registry_schema.sql")
    store.close()
    store.close()
