import adapt.persistence.repository as repo_mod
from adapt.persistence.registry import RepositoryRegistry
from adapt.persistence.repository import DataRepository


def test_repository_does_not_use_external_radar_location_lookup(tmp_path, monkeypatch):
    def _should_not_be_called(_radar: str):
        raise AssertionError("_lookup_radar_location_pyart should not be called")

    if hasattr(repo_mod, "_lookup_radar_location_pyart"):
        monkeypatch.setattr(repo_mod, "_lookup_radar_location_pyart", _should_not_be_called)
    repo = DataRepository(run_id="TESTRUN", base_dir=tmp_path, radar="KPOE", config=None)
    try:
        radars = repo.registry.list_radars()
        row = radars[radars["radar"] == "KPOE"].iloc[0]
        assert row["location_lat"] is None
        assert row["location_lon"] is None
    finally:
        repo.close()


def test_repository_does_not_overwrite_existing_radar_location(tmp_path, monkeypatch):
    registry = RepositoryRegistry.get_instance(tmp_path)
    registry.register_radar("KPOE", lat=9.0, lon=10.0)

    def _should_not_be_called(_radar: str):
        raise AssertionError("_lookup_radar_location_pyart should not be called")

    if hasattr(repo_mod, "_lookup_radar_location_pyart"):
        monkeypatch.setattr(repo_mod, "_lookup_radar_location_pyart", _should_not_be_called)
    repo2 = DataRepository(run_id="TESTRUN2", base_dir=tmp_path, radar="KPOE", config=None)
    try:
        radars = repo2.registry.list_radars()
        row = radars[radars["radar"] == "KPOE"].iloc[0]
        assert float(row["location_lat"]) == 9.0
        assert float(row["location_lon"]) == 10.0
    finally:
        repo2.close()
