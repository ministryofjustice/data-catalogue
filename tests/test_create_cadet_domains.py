import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

from ingestion.create_cadet_databases_source.config import CreateCadetDatabasesConfig
from ingestion.create_cadet_databases_source.source import CreateCadetDatabases


def test_creating_domains_from_s3():
    source = CreateCadetDatabases(
        ctx=PipelineContext(run_id="domain-source-test"),
        config=CreateCadetDatabasesConfig(
            manifest_s3_uri="s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
        ),
    )

    results = list(source.get_workunits())

    domain_creation_events = results[:4]
    domains = [event.metadata.aspect.name for event in domain_creation_events]
    domains.sort()
    assert domains == ["Courts", "HQ", "Prison", "Probation"]

    # 6 events are created per database, we'll just test one
    # (create container, update status, add platform, add subtype, associate container with domain)
    assert results[4].metadata.aspect.customProperties.get("database")
    assert results[6].metadata.aspect.platform == builder.make_data_platform_urn(
        platform="dbt"
    )
    assert DatasetContainerSubTypes.DATABASE in results[7].metadata.aspect.typeNames
    assert results[8].metadata.entityUrn == results[4].metadata.entityUrn
    domain_result_4 = (
        results[4].metadata.aspect.customProperties.get("database").split("_")[0]
    )
    assert (
        builder.make_domain_urn(domain_result_4) in results[8].metadata.aspect.domains
    )
