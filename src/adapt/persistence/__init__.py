# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The Adapt data store: registry + per-collection catalog/products/objects.

The store layout is created by ``adapt init`` (see :func:`init_store`); the
runtime composes these components, and every consumer reads through
``adapt.api.StoreClient``.
"""

from adapt.persistence.errors import AlreadyInitializedError, StoreError
from adapt.persistence.store import Collection, Store, init_store
from adapt.persistence.store_registry import StoreRegistry

__all__ = [
    "AlreadyInitializedError",
    "Collection",
    "Store",
    "StoreError",
    "StoreRegistry",
    "init_store",
]
