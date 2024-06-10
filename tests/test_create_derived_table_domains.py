from datahub.ingestion.api.common import PipelineContext

from ingestion.create_derived_table_databases_source.config import (
    CreateDerivedTableDatabasesConfig,
)
from ingestion.create_derived_table_databases_source.source import (
    CreateDerivedTableDatabases,
)
import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes


def test_creating_domains_from_s3():
    source = CreateDerivedTableDatabases(
        ctx=PipelineContext(run_id="domain-source-test"),
        config=CreateDerivedTableDatabasesConfig(
            manifest_s3_uri="s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
        ),
    )

    results = list(source.get_workunits())

    assert results
    assert len(results) == 24

    domain_creation_events = results[:4]
    domains = [event.metadata.aspect.name for event in domain_creation_events]
    domains.sort()
    assert domains == ["courts", "hq", "prison", "probation"]

    # 5 events are created per database, we'll just test one
    # (create container, update status, add platform, add subtype, associate domain)
    assert results[4].metadata.aspect.customProperties["database"] == "prison_database"
    assert results[6].metadata.aspect.platform == builder.make_data_platform_urn(platform="dbt")
    assert DatasetContainerSubTypes.DATABASE in results[7].metadata.aspect.typeNames
    assert builder.make_domain_urn(domain="prison") in results[8].metadata.aspect.domains
    assert results[8].metadata.entityUrn == results[4].metadata.entityUrn
