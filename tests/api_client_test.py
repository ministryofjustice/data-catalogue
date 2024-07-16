import pytest
from ingestion.justice_data_source.api_client import JusticeDataAPIClient


@pytest.fixture
def client():
    return JusticeDataAPIClient("https://data.justice.gov.uk/api/")


def test_list_all(client):
    response = client.list_all()
    assert response
