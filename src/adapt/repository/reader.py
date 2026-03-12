"""Repository reader — consumer-facing read interface.

Thin facade over DataRepository for reading stored artifacts.
External consumers should use adapt.api.DataClient for queries.
This reader is for internal module use (e.g., reading previous outputs).
"""

from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from adapt.core.data_repository import DataRepository


class RepositoryReader:
    """Read pipeline outputs from the DataRepository.

    Parameters
    ----------
    repository : DataRepository
        The underlying storage backend.
    """

    def __init__(self, repository: "DataRepository") -> None:
        self.repository = repository

    def list_items(self, item_type: Optional[str] = None) -> List[dict]:
        """List registered artifacts, optionally filtered by type."""
        return self.repository.radar_catalog.list_items(item_type=item_type)

    def get_latest(self, item_type: str) -> Optional[dict]:
        """Return the most recently registered item of the given type."""
        items = self.list_items(item_type=item_type)
        if not items:
            return None
        return sorted(items, key=lambda x: x.get("created_at", ""), reverse=True)[0]
