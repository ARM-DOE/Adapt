# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Store-level exceptions, shared by every persistence component."""

__all__ = ["AlreadyInitializedError", "StoreError"]


class StoreError(Exception):
    """A store-level operation failed."""


class AlreadyInitializedError(StoreError):
    """The root already contains an initialized store."""
