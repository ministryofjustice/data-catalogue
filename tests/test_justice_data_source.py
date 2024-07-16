import vcr
from datahub.ingestion.api.common import PipelineContext

from ingestion.justice_data_source.config import JusticeDataAPIConfig
from ingestion.justice_data_source.source import JusticeDataAPISource


def test_host_port_parsing():
    examples = [
        "http://localhost:8080",
        "localhost",
        "192.168.0.1",
        "https://192.168.0.1/",
    ]

    for example in examples:
        config_dict = {"base_url": example}
        config = JusticeDataAPIConfig.parse_obj(config_dict)
        assert config.base_url == example


def test_ingest():
    source = JusticeDataAPISource(
        ctx=PipelineContext(run_id="justice-api-source-test"),
        config=JusticeDataAPIConfig(base_url="https://data.justice.gov.uk/api"),
    )

    with vcr.use_cassette("tests/fixtures/vcr_cassettes/fetch_justice_data.yaml"):
        result = list(source.get_workunits())

        assert result

        first_chart = result[0].metadata.proposedSnapshot
        assert (
            first_chart.urn
            == "urn:li:chart:(justice-data,legal-aid-ecf-applicationsgranted)"
        )
        chartinfo = first_chart.aspects[1]
        assert (
            chartinfo.chartUrl
            == "https://data.justice.gov.uk/legalaid/legal-aid-ecf/legal-aid-ecf-applicationsgranted"
        )
        assert chartinfo.title == "Applications granted"
        assert (
            chartinfo.description
            == '<p class="govuk-body">Applications determination granted.</p>'
        )
