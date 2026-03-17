"""Core infrastructure for Adapt radar processing pipeline.

This module provides centralized data management through the DataRepository class.
"""

from adapt.persistence.repository import DataRepository, ProductType

__all__ = ['DataRepository', 'ProductType']
