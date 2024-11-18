from datetime import datetime

import pytest
import vcr

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
        "ownerEmail": "somebody@justice.gov.uk",
    },
]


@pytest.fixture
def client(default_owner_email):
    return JusticeDataAPIClient("https://data.justice.gov.uk/api", default_owner_email)


def test_list_all(client):
    response = client.list_all()
    assert response


ids_with_prison_domain = [
    "prison-staff-sickness",
    "prison-staff-in-post",
    "releases-in-error",
    "crowding",
    "performance-band",
    "prison-opcap",
    "rotl",
    "releases",
    "receptions",
    "population-remand",
    "population-life",
    "population-ipp",
    "population",
    "employment-on-release",
    "accommodation-on-release",
    "prisoner-work-hours",
    "prisoners-working",
    "alcohol-drug-treatment",
    "random-mandatory-drug-testing-nps",
    "random-mandatory-drug-testing",
    "risk-management-audit",
    "concerted-indiscipline",
    "hostage",
    "barricades",
    "self-inflicted-deaths",
    "self-harm-rate",
    "assaults-rate-staff",
    "assaults-rate-prisoner",
    "security-audit",
    "temporary-release-failures",
    "absconds",
    "escapes",
]


def test_list_all_domain_assignment(client):
    with vcr.use_cassette("tests/fixtures/vcr_cassettes/fetch_justice_data.yaml"):
        client._id_to_domain_mapping = {
            "prisons": "prison",
            "incidents-at-height": "prison incidents",
        }
        results = client.list_all(exclude_id_list=["justice-in-numbers"])

        for result in results:
            if result["id"] in ids_with_prison_domain:
                assert result["domain"] == "Prison"
            elif result["id"] == "incidents-at-height":
                assert result["domain"] == "Prison incidents"


def test_get_publication_metadata(client, default_owner_email):
    # ons missing data
    ids = [
        "courts-civil",
        "community-performance",
        "ons-crime",
        "ons-crime2",
        "ons-crime3",
    ]
    client.publication_details = {pub["id"]: pub for pub in test_published_details}
    for i, id in enumerate(ids):
        last_updated, refresh_period, owner_email = client._get_publication_metadata(
            id
        )
        if test_published_details[i].get("currentPublishDate"):
            expected_updated_timestamp = datetime.strptime(
                test_published_details[i].get("currentPublishDate"),
                "%d %B %Y",
            )
        else:
            expected_updated_timestamp = None
        assert last_updated == expected_updated_timestamp
        assert refresh_period == test_published_details[i].get("frequency")
        assert owner_email == test_published_details[i].get(
            "ownerEmail", default_owner_email
        )


@pytest.mark.parametrize(
    "id_to_domain_mapping, datahub_domain_list, expect_error",
    [
        (
            {"prisons": "prison", "probation": "probation"},
            ["prison", "probation"],
            False,
        ),
        (
            {"prisons": "prison", "probation": "probation", "legal-aid": "LAA"},
            ["prison", "probation"],
            True,
        ),
    ],
)
def test_validate_domains(
    id_to_domain_mapping, datahub_domain_list, expect_error, client
):

    client._id_to_domain_mapping = id_to_domain_mapping
    if expect_error:
        with pytest.raises(Exception):
            client.validate_domains(datahub_domain_list)
    else:
        assert client.validate_domains(datahub_domain_list)


def test_list_publications(client):
    with vcr.use_cassette(
        "tests/fixtures/vcr_cassettes/fetch_justice_data_publications.yaml"
    ):
        assert client.list_publications()[:3] == [
            {
                "id": "courts-civil",
                "name": "Civil justice statistics",
                "indexUri": "https://www.gov.uk/government/collections/civil-justice-statistics-quarterly",
                "frequency": "Quarterly",
                "source": "MOJ",
                "documentType": "Accredited official statistics",
                "description": "Volume of civil and judicial review cases dealt with by the courts over time and the overall timeliness of these cases. ",
                "ownerEmail": "CAJS@justice.gov.uk",
                "currentPublishDate": "5 September 2024",
                "currentPublishDateAsDate": None,
                "nextPublishDate": "5 December 2024 9:30am",
                "nextPublishDateAsDateTime": "2024-12-05T09:30:00Z",
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
                "ownerEmail": "communityperformanceenquiries@justice.gov.uk",
                "currentPublishDate": "25 July 2024",
                "currentPublishDateAsDate": "2024-07-25T00:00:00+00:00",
                "nextPublishDate": None,
                "nextPublishDateAsDateTime": None,
                "sourceName": "Ministry of Justice",
            },
            {
                "id": "ons-crime",
                "name": "Crime in England and Wales",
                "indexUri": "https://www.ons.gov.uk/peoplepopulationandcommunity/crimeandjustice/datasets/crimeinenglandandwalesappendixtables",
                "frequency": "Quarterly",
                "source": "ONS",
                "documentType": "Official Statistics",
                "description": "Crime against households and adults using data from\xa0police recorded crime and the Crime Survey for England and Wales.",
                "ownerEmail": "crimestatistics@ons.gov.uk",
                "currentPublishDate": "24 October 2024",
                "currentPublishDateAsDate": "2024-10-24T00:00:00+00:00",
                "nextPublishDate": None,
                "nextPublishDateAsDateTime": None,
                "sourceName": "Office of National Statistics",
            },
        ]
