# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Core infrastructure for Adapt radar processing pipeline.

This module provides centralized data management through the DataRepository class.
"""

from adapt.persistence.repository import DataRepository, ProductType

__all__ = ['DataRepository', 'ProductType']
