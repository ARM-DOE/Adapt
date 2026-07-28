# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Persistence spec types are frozen value types with zero dependencies."""

import dataclasses

import pytest

pytestmark = pytest.mark.unit


def test_specs_are_frozen_value_types():
    from adapt.contracts import RegisterFileArtifact, SqliteTable

    spec = RegisterFileArtifact(key="grid_nc_path", product_type="gridded3d", producer="ingest")
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.key = "other"
    table = SqliteTable(key="rows", table="t", primary_key=("run_id",))
    assert table.index_columns == ()


def test_all_spec_types_exported():
    from adapt import contracts

    for name in (
        "RegisterFileArtifact",
        "NetcdfArtifact",
        "ParquetArtifact",
        "TrackTablesWrite",
        "SqliteTable",
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
