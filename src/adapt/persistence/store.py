# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Store lifecycle: initialization and opening of the on-disk Adapt data store.

The layout is created only by :func:`init_store` (surfaced as ``adapt init``);
the pipeline refuses to run against an uninitialized root::

    {root}/
    ├── registry.db      # collections + run lifecycle (store_registry.sql)
    ├── logs/
    └── collections/     # provisioned per collection at run start
"""

import hashlib
import json
import sqlite3
from pathlib import Path

from adapt.persistence.collection_catalog import Catalog
from adapt.persistence.errors import AlreadyInitializedError, StoreError
from adapt.persistence.sqlite_store import SqliteStore

__all__ = [
    "REGISTRY_FILENAME",
    "AlreadyInitializedError",
    "Collection",
    "Store",
    "StoreError",
    "init_store",
]

REGISTRY_FILENAME = "registry.db"
_LEGACY_REGISTRY = "adapt_registry.db"


def _reject_legacy_root(root_path: Path) -> None:
    if (root_path / _LEGACY_REGISTRY).exists():
        raise StoreError(
            f"{root_path} uses the obsolete pre-store layout ({_LEGACY_REGISTRY}). "
            "Recreate the store: move the old data aside and run 'adapt init'."
        )


class Collection:
    """One collection's on-disk domain: catalog.db + products.db + decisions.db + objects/.

    ``decisions.db`` is the analysis-only decision log (segmentation and
    tracking decisions); the pipeline never reads it.
    """

    def __init__(self, collection_id: str, path: Path, catalog: Catalog) -> None:
        self.collection_id = collection_id
        self.path = path
        self.catalog = catalog
        self.objects_dir = path / "objects"
        self.products_path = path / "products.db"
        self.decisions_path = path / "decisions.db"

    def close(self) -> None:
        self.catalog.close()


class Store:
    """An initialized on-disk Adapt data store. Obtain via :meth:`open`."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._collections: dict[str, Collection] = {}

    @classmethod
    def open(cls, root: str | Path) -> "Store":
        """Open an initialized store, failing loudly on legacy or bare roots."""
        root_path = Path(root).resolve()
        _reject_legacy_root(root_path)
        if not (root_path / REGISTRY_FILENAME).exists():
            raise StoreError(
                f"No Adapt store at {root_path}: {REGISTRY_FILENAME} not found. "
                "Run 'adapt init' first."
            )
        return cls(root_path)

    def collection(self, collection_id: str) -> Collection:
        """Provision (idempotently), validate, and return the collection's domain."""
        if collection_id in self._collections:
            return self._collections[collection_id]

        collection_dir = self.root / "collections" / collection_id
        collection_dir.mkdir(exist_ok=True)
        (collection_dir / "objects").mkdir(exist_ok=True)
        products = SqliteStore(collection_dir / "products.db", "collection_products.sql")
        products.close()
        decisions = SqliteStore(collection_dir / "decisions.db", "collection_decisions.sql")
        decisions.close()
        self.validate_collection(collection_id)

        coll = Collection(collection_id, collection_dir, Catalog(collection_dir))
        self._collections[collection_id] = coll
        return coll

    def validate_collection(self, collection_id: str) -> None:
        """Raise when catalog.db and products.db schema snapshots disagree."""
        collection_dir = self.root / "collections" / collection_id
        catalog_fps = _snapshot_fingerprints(collection_dir / "catalog.db")
        products_fps = _snapshot_fingerprints(collection_dir / "products.db")
        for table in sorted(set(catalog_fps) | set(products_fps)):
            catalog_fp = catalog_fps.get(table, "absent")
            products_fp = products_fps.get(table, "absent")
            if catalog_fp != products_fp:
                raise StoreError(
                    f"Collection '{collection_id}' schema snapshots disagree for table "
                    f"'{table}': catalog {catalog_fp} vs products {products_fp} — "
                    "the store is inconsistent; recreate the collection"
                )

    def close(self) -> None:
        """Close every collection opened through this store."""
        for coll in self._collections.values():
            coll.close()
        self._collections.clear()


def _snapshot_fingerprints(db_path: Path) -> dict[str, str]:
    """Per-table sha256 fingerprint of each table_schemas snapshot row."""
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM table_schemas").fetchall()
    finally:
        conn.close()
    return {
        row["table_name"]: hashlib.sha256(
            json.dumps(dict(row), sort_keys=True).encode()
        ).hexdigest()[:16]
        for row in rows
    }


def init_store(root: str | Path) -> Path:
    """Create the store layout at ``root`` and return the resolved root.

    Creates exactly ``registry.db``, ``logs/``, and ``collections/``. Raises
    :class:`AlreadyInitializedError` if a store exists, and :class:`StoreError`
    on a root using the obsolete pre-store layout.
    """
    root_path = Path(root).resolve()
    if (root_path / REGISTRY_FILENAME).exists():
        raise AlreadyInitializedError(f"Store already initialized at {root_path}")
    _reject_legacy_root(root_path)

    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "logs").mkdir()
    (root_path / "collections").mkdir()

    registry = SqliteStore(root_path / REGISTRY_FILENAME, "store_registry.sql")
    registry.close()
    return root_path
