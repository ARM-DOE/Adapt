# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""`adapt postprocess` CLI: grammar + dispatch to the PostProcessor."""

import argparse
import sqlite3

import pandas as pd
import pytest

import adapt.cli as cli
from adapt.contracts import ProductTableWrite
from adapt.execution.module_registry import registry
from adapt.modules.base import POSTPROCESS_PHASE, BaseModule
from adapt.persistence.store import Store, init_store
from adapt.persistence.store_registry import RunStart, StoreRegistry

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


class _FakePostModule(BaseModule):
    name = "fake_post"
    pipeline_phase = POSTPROCESS_PHASE
    inputs = ["run_id"]
    outputs = ["fake_rows"]
    persistence = (
        ProductTableWrite(key="fake_rows", table="fake_ext", primary_key=("run_id", "cell_uid")),
    )

    def run(self, context: dict) -> dict:
        return {"fake_rows": pd.DataFrame([{"run_id": context["run_id"], "cell_uid": "z", "v": 2}])}


def test_parser_accepts_repository_and_module_list():
    parser = argparse.ArgumentParser()
    cli._build_postprocess_parser(parser)

    args = parser.parse_args(["--repository", "/data/case", "--module", "lma", "dualpol"])

    assert args.repository == "/data/case"
    assert args.module == ["lma", "dualpol"]


def test_repository_defaults_to_cwd():
    parser = argparse.ArgumentParser()
    cli._build_postprocess_parser(parser)

    args = parser.parse_args(["--module", "lma"])

    assert args.repository == "."


def test_postprocess_cmd_runs_module_and_writes_table(monkeypatch, internal_config, temp_dir):
    root = init_store(temp_dir / "store")
    store = Store.open(root)
    collection = store.collection("TEST_RADAR")
    reg = StoreRegistry.get_instance(root)
    reg.register_collection("TEST_RADAR", source_kind="nexrad")
    run_config = internal_config.model_copy(update={"run_id": "CLITEST1"})
    reg.begin_run(
        RunStart(
            run_id="CLITEST1",
            collection_id="TEST_RADAR",
            config_hash="h",
            config_json=run_config.model_dump_json(),
            pipeline_version="0",
            environment_json="{}",
        )
    )
    registry.register(_FakePostModule)
    try:
        args = argparse.Namespace(
            repository=str(root),
            module=["fake_post"],
            input_dir=None,
            config=None,
            verbose=False,
        )
        cli._postprocess_cmd(args)

        conn = sqlite3.connect(collection.products_path)
        try:
            rows = conn.execute("SELECT cell_uid, v FROM fake_ext").fetchall()
        finally:
            conn.close()
        assert rows == [("z", 2)]
    finally:
        registry.unregister("fake_post")
        store.close()
