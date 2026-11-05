# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Persistence spec types are frozen value types with zero dependencies."""

import dataclasses

import pytest

pytestmark = pytest.mark.unit


def test_specs_are_frozen_value_types():
    from adapt.contracts import NetcdfArtifact, ProductTableWrite

    spec = ProductTableWrite(key="cell_stats", table="cell_stats", primary_key=("run_id",))
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.key = "other"
    nc = NetcdfArtifact(key="ds", product_type="gridded3d", producer="ingest", description="d")
    with pytest.raises(dataclasses.FrozenInstanceError):
        nc.key = "other"
    assert ProductTableWrite(key="r", table="t", primary_key=("run_id",)).index_columns == ()


def test_all_spec_types_exported():
    from adapt import contracts

    for name in (
        "ProductTableWrite",
        "NetcdfArtifact",
        "TrackTablesWrite",
        "DecisionTableWrite",
        "PersistenceSpec",
        "PersistenceMeta",
    ):
        assert hasattr(contracts, name), f"adapt.contracts missing {name}"


def test_contracts_persistence_imports_stdlib_only():
    import ast

    import adapt.contracts.persistence as mod

    with open(mod.__file__, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported <= {"dataclasses", "datetime"}, f"non-stdlib imports: {imported}"
