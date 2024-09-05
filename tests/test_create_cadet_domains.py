import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

from ingestion.create_cadet_databases_source.config import CreateCadetDatabasesConfig
from ingestion.create_cadet_databases_source.source import CreateCadetDatabases
from ingestion.ingestion_utils import format_domain_name


class TestCreateCadetDatabases:
    def setup_method(self):
        source = CreateCadetDatabases(
            ctx=PipelineContext(run_id="domain-source-test"),
            config=CreateCadetDatabasesConfig(
                manifest_s3_uri="s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
            ),
        )
        self.results = list(source.get_workunits())

    def test_creating_domains_from_s3(self):
        domain_creation_events = self.results[:4]
        domains = [event.metadata.aspect.name for event in domain_creation_events]
        domains.sort()
        assert domains == ["Courts", "HQ", "Prison", "Probation"]

        # 6 events are created per database, we'll just test one
        # (create container, update status, add platform, add subtype, associate container with domain)
        assert self.results[4].metadata.aspect.customProperties.get("database")
        assert self.results[6].metadata.aspect.platform == builder.make_data_platform_urn(
            platform="dbt"
        )
        assert DatasetContainerSubTypes.DATABASE in self.results[7].metadata.aspect.typeNames
        assert self.results[8].metadata.entityUrn == self.results[4].metadata.entityUrn
        domain_result_4 = (
            self.results[4].metadata.aspect.customProperties.get("database").split("_")[0]
        )
        assert (
            builder.make_domain_urn(format_domain_name(domain_result_4))
            in self.results[8].metadata.aspect.domains
        )

    def test_datasets_are_assigned_to_domains(self):
        # This is the first event which should associate a dataset with a database
        assert self.results[28].metadata.entityType == "dataset"
        assert self.results[28].metadata.changeType == "UPSERT"
        assert self.results[28].metadata.aspect.domains
