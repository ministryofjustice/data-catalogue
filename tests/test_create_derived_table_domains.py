from datahub.ingestion.api.common import PipelineContext

from ingestion.create_derived_table_domains_source.config import (
    CreateDerivedTableDomainsConfig,
)
from ingestion.create_derived_table_domains_source.source import (
    CreateDerivedTableDomains,
)


def test_creating_domains_from_s3():
    source = CreateDerivedTableDomains(
        ctx=PipelineContext(run_id="domain-source-test"),
        config=CreateDerivedTableDomainsConfig(
            manifest_s3_uri="s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
        ),
    )

    results = list(source.get_workunits())

    assert results

    domains = [result.metadata.aspect.name for result in results]
    domains.sort()

    assert domains == ["courts", "hq", "prison", "probation"]
