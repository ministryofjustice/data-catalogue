from datetime import datetime

import pytest

from ingestion.justice_data_source.api_client import JusticeDataAPIClient

test_published_details = [
    {
        "id": "courts-civil",
        "name": "Civil justice statistics",
        "indexUri": "https://www.gov.uk/government/collections/civil-justice-statistics-quarterly",
        "frequency": "Quarterly",
        "source": "MOJ",
        "documentType": "Accredited official statistics",
        "description": "Volume of civil and judicial review cases dealt with by the courts over time and the overall timeliness of these cases. ",
        "currentPublishDate": "6 June 2024",
        "currentPublishDateAsDate": None,
        "nextPublishDate": "5 September 2024 9:30am",
        "nextPublishDateAsDateTime": "2024-09-05T09:30:00Z",
        "sourceName": "Ministry of Justice",
    },
    {
        "id": "community-performance",
        "name": "Community performance annual",
        "indexUri": "https://www.gov.uk/government/collections/prison-and-probation-trusts-performance-statistics#community-performance-statistics",
        "frequency": "Annual",
        "source": "MOJ",
        "documentType": "Official Statistics",
        "description": "An annual release of performance statistics for the Probation Service, incorporating Probation Service and Commissioned Rehabilitative Services performance.",
        "currentPublishDate": "27 July 2023",
        "currentPublishDateAsDate": "2023-07-27T00:00:00+00:00",
        "nextPublishDate": "25 July 2024 9:30am",
        "nextPublishDateAsDateTime": "2024-07-25T09:30:00Z",
        "sourceName": "Ministry of Justice",
    },
    {
        "id": "ons-crime",
        "name": "Crime in England and Wales",
        "currentPublishDate": None,
        "currentPublishDateAsDate": "2024-04-25T00:00:00+00:00",
        "nextPublishDate": "24 July 2024 9:30am",
        "nextPublishDateAsDateTime": "2024-07-24T09:30:00Z",
        "sourceName": "Office of National Statistics",
    },
    {
        "id": "ons-crime2",
        "name": "Crime in England and Wales",
        "currentPublishDate": "",
        "currentPublishDateAsDate": "2024-04-25T00:00:00+00:00",
        "nextPublishDate": "24 July 2024 9:30am",
        "nextPublishDateAsDateTime": "2024-07-24T09:30:00Z",
        "sourceName": "Office of National Statistics",
    },
    {
        "id": "ons-crime3",
        "name": "Crime in England and Wales",
        "currentPublishDate": "",
        "currentPublishDateAsDate": "2024-04-25T00:00:00+00:00",
        "nextPublishDate": "24 July 2024 9:30am",
        "nextPublishDateAsDateTime": "2024-07-24T09:30:00Z",
        "sourceName": "Office of National Statistics",
    },
]


@pytest.fixture
def client():
    return JusticeDataAPIClient("https://data.justice.gov.uk/api/")


def test_list_all(client):
    response = client.list_all()
    assert response


def test_get_publication_timings(client):
    # ons missing data
    ids = [
        "courts-civil",
        "community-performance",
        "ons-crime",
        "ons-crime2",
        "ons-crime3",
    ]
    # client.publication_details = MagicMock(return_value=test_published_details)
    client.publication_details = test_published_details
    for i, id in enumerate(ids):
        last_updated, refresh_frequency = client._get_publication_timings(id)
        if test_published_details[i].get("currentPublishDate"):
            expected_updated_timestamp = datetime.strptime(
                test_published_details[i].get("currentPublishDate"),
                "%d %B %Y",
            ).timestamp()
        else:
            expected_updated_timestamp = None
        assert last_updated == expected_updated_timestamp
        assert refresh_frequency == test_published_details[i].get("frequency")
