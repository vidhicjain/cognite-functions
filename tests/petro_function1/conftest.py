import pytest

from cognite.client.testing import monkeypatch_cognite_client


@pytest.fixture
def cognite_client_mock():
    with monkeypatch_cognite_client() as client:
        yield client


@pytest.fixture
def mocked_data():
    # return {"lovely-parameter": True, "greeting": "World"}
    return {
        "asset_external_name": "Site:3W:PDG-M33-158",
        "asset_time_series_external_name": "Site:3W:PDG-M33-158_pressure",
        "asset_time_series_id": 1495620710607510,
        "event_one_file_name": "DRAWN_00009.csv",
        "file_id": 1996920632623235,
    }
