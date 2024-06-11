from datahub.ingestion.api.common import PipelineContext

from ingestion.assign_cadet_databases_source.config import (
    AssignCadetDatabasesConfig,
)
from ingestion.assign_cadet_databases_source.source import (
    AssignCadetDatabases,
)
import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes


def test_assigning_domains_from_s3():
    source = AssignCadetDatabases(
        ctx=PipelineContext(run_id="domain-source-test"),
        config=AssignCadetDatabasesConfig(
            manifest_s3_uri="s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
        ),
    )
    expected_mappings = {
        'urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.prison_database.table1,PROD)': 'urn:li:container:b17e173b8950dee2415a3119fb7c9d12',
        'urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.probation_database.table1,PROD)': 'urn:li:container:ea9744b8004d93b716687bab12438c90',
        'urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.courts_data.table1,PROD)': 'urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e',
        'urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.hq_database.table1,PROD)': 'urn:li:container:48e5e41ce461da41f0333b67a322fb99'
    }

    results = list(source.get_workunits())

    assert results
    assert len(results) == 4
    assert (
        expected_mappings[mcp.metadata.entityUrn] == mcp.metadata.aspect.container
        for mcp in results
    )


