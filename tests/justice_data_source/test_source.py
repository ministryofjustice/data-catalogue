import pytest
import vcr
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from utils import WorkunitInspector

from ingestion.justice_data_source.config import JusticeDataAPIConfig
from ingestion.justice_data_source.source import JusticeDataAPISource


def test_host_port_parsing(default_owner_email):
    examples = [
        "http://localhost:8080",
        "localhost",
        "192.168.0.1",
        "https://192.168.0.1/",
    ]

    for example in examples:
        config_dict = {"base_url": example, "default_owner_email": default_owner_email}
        config = JusticeDataAPIConfig.parse_obj(config_dict)
        assert config.base_url == example


@pytest.fixture()
def source_with_mock_justice_data_api(default_owner_email):

    with vcr.use_cassette("tests/fixtures/vcr_cassettes/fetch_justice_data.yaml"):
        yield JusticeDataAPISource(
            ctx=PipelineContext(run_id="justice-api-source-test"),
            config=JusticeDataAPIConfig(
                base_url="https://data.justice.gov.uk/api",
                default_owner_email=default_owner_email,
            ),
            validate_subject_areas=False,
        )


def test_workunits_created(source_with_mock_justice_data_api):
    assert source_with_mock_justice_data_api.get_workunits()


def test_chart(source_with_mock_justice_data_api):
    metadata = WorkunitInspector(source_with_mock_justice_data_api.get_workunits())

    chart_aspects = metadata.entity(
        "urn:li:chart:(justice-data,legal-aid-ecf-applicationsgranted)"
    )
    assert chart_aspects

    chartinfo = chart_aspects.aspect("chartInfo")
    assert (
        chartinfo.chartUrl
        == "https://data.justice.gov.uk/legalaid/legal-aid-ecf/legal-aid-ecf-applicationsgranted"
    )
    assert chartinfo.title == "Applications granted"
    assert (
        chartinfo.description
        == '<p class="govuk-body">Applications determination granted.</p>'
    )

    assert chartinfo.customProperties == {
        "security_classification": "Official - For public release",
        "dc_access_requirements": "",
        "refresh_period": "Quarterly",
        "dc_team_email": "not.me@justice.gov.uk",
    }


def test_tags(source_with_mock_justice_data_api):
    metadata = WorkunitInspector(source_with_mock_justice_data_api.get_workunits())

    tags = metadata.entity(
        "urn:li:chart:(justice-data,legal-aid-ecf-applicationsgranted)"
    ).tag_names

    assert set(tags) == {"urn:li:tag:dc_display_in_catalogue", "urn:li:tag:General"}


def test_dashboard(source_with_mock_justice_data_api):
    metadata = WorkunitInspector(source_with_mock_justice_data_api.get_workunits())
    dashboard = metadata.entity("urn:li:dashboard:(justice-data,Justice Data)")
    assert dashboard

    # make all chart urns list
    chart_urns = [chart.urn for chart in metadata.charts]

    assert dashboard.aspect("dashboardInfo").charts == chart_urns
