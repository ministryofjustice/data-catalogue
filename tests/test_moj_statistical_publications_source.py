from collections import defaultdict
from unittest.mock import patch

import pytest
import vcr
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    StatusClass,
    SubTypesClass,
)

from ingestion.moj_statistical_publications_source.config import (
    MojPublicationsAPIConfig,
    MojPublicationsAPIParams,
)
from ingestion.moj_statistical_publications_source.source import (
    MojPublicationsAPISource,
)


def test_host_port_parsing(default_contact_email):
    examples = [
        "http://localhost:8080",
        "localhost",
        "192.168.0.1",
        "https://192.168.0.1/",
    ]

    for example in examples:
        config_dict = {
            "base_url": example,
            "default_contact_email": default_contact_email,
            "params": MojPublicationsAPIParams(
                filter_organisations=["ministry-of-justice"],
                filter_content_store_document_type=[
                    "national_statistics",
                ],
            ),
        }
        config = MojPublicationsAPIConfig.parse_obj(config_dict)
        assert config.base_url == example


@pytest.fixture()
def mock_justice_publication_api(default_contact_email, publication_mappings):
    vcr_decode = vcr.VCR(
        decode_compressed_response=True,
    )

    with vcr_decode.use_cassette(
        "tests/fixtures/vcr_cassettes/fetch_moj_publications.yaml"
    ):
        source = MojPublicationsAPISource(
            ctx=PipelineContext(run_id="moj-publications-api-source-test"),
            config=MojPublicationsAPIConfig(
                base_url="https://www.gov.uk/api",
                default_contact_email=default_contact_email,
                collections_to_exclude=["offender-management-statistics-quarterly--3"],
                params=MojPublicationsAPIParams(
                    filter_organisations=["ministry-of-justice"],
                    filter_content_store_document_type=[
                        "national_statistics",
                        "official_statistics",
                    ],
                ),
            ),
        )
        # these are likely to change so we need to mock them
        with patch.object(
            source,
            "_id_to_metadata_mapping",
            new=publication_mappings,
        ):
            results = list(source.get_workunits())

        return results


def test_workunits_created(mock_justice_publication_api):
    assert mock_justice_publication_api


def test_workunits(mock_justice_publication_api):
    """test creating of container and datasets aspects is as expected"""

    workunits_by_aspect_type = defaultdict(list)
    for wu in mock_justice_publication_api:
        aspect_type = type(wu.metadata.aspect)
        workunits_by_aspect_type[aspect_type].append(wu)

    container_events = workunits_by_aspect_type[ContainerPropertiesClass]
    status_events = workunits_by_aspect_type[StatusClass]
    dataset_events = workunits_by_aspect_type[DatasetPropertiesClass]
    platform_events = workunits_by_aspect_type[DataPlatformInstanceClass]
    sub_types_events = workunits_by_aspect_type[SubTypesClass]
    tags_events = workunits_by_aspect_type[GlobalTagsClass]

    # We expect 51 containers
    assert len(container_events) == 51
    # datasets and containers have platform and status events
    assert (
        len(platform_events)
        == len(status_events)
        == len(sub_types_events)
        == (len(container_events) + len(dataset_events))
    )

    assert [
        c
        for c in container_events
        if c.id
        == "urn:li:container:1b4be655114e80be353ae337f9a3a315-containerProperties"
    ][0].metadata.aspect.customProperties == {
        "platform": "GOV.UK",
        "instance": "ministry-of-justice-publications",
        "env": "prod",
        "database": "Electronic Monitoring Statistics Publication",
        "dc_access_requirements": "",
        "security_classification": "Official - For public release",
        "dc_team_email": "ppas_statistics@justice.gov.uk",
    }

    tags = [tag.tag for tag in tags_events[0].metadata.aspect.tags]
    assert "urn:li:tag:dc_display_in_catalogue" in tags

    assert [
        d
        for d in dataset_events
        if d.id
        == "urn:li:dataset:(urn:li:dataPlatform:GOV.UK,ministry-of-justice-publications.proven-reoffending-statistics-july-2022-to-september-2022,PROD)-datasetProperties"
    ][0].metadata.aspect.customProperties == {
        "dc_access_requirements": "",
        "security_classification": "Official - For public release",
        "dc_team_email": "CAJS@justice.gov.uk",
    }
