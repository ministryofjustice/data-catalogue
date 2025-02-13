import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from utils import WorkunitInspector

from ingestion.create_cadet_databases_source.config import CreateCadetDatabasesConfig
from ingestion.create_cadet_databases_source.source import CreateCadetDatabases


class TestCreateCadetDatabases:
    def setup_method(self):
        source = CreateCadetDatabases(
            ctx=PipelineContext(run_id="domain-source-test"),
            config=CreateCadetDatabasesConfig(
                manifest_s3_uri="s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
                database_metadata_s3_uri="s3://test_bucket/prod/run_artefacts/latest/target/database_metadata.json",
            ),
        )
        self.results = WorkunitInspector(source.get_workunits())

    def test_creating_domains_from_s3(self):
        urns = [
            "urn:li:container:27c5c4df57bf429bf9e56e51b30003ed",
            "urn:li:container:ea9744b8004d93b716687bab12438c90",
            "urn:li:container:48e5e41ce461da41f0333b67a322fb99",
            "urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e",
            "urn:li:container:b17e173b8950dee2415a3119fb7c9d12",
        ]

        for urn in urns:
            entity = self.results.entity(urn)
            assert entity

            assert entity.aspect("containerProperties")
            assert entity.aspect("status")
            assert entity.aspect("dataPlatformInstance")
            assert entity.aspect("subTypes")
            assert entity.aspect("globalTags")

            assert entity.aspect("containerProperties").customProperties.get("database")

            assert (
                entity.aspect("dataPlatformInstance").platform
            ) == builder.make_data_platform_urn(platform="dbt")
            assert (
                DatasetContainerSubTypes.DATABASE in entity.aspect("subTypes").typeNames
            )

        assert self.results.entity("urn:li:corpuser:some.one").aspect("corpUserInfo")
        assert self.results.entity("urn:li:corpuser:some.team").aspect("corpUserInfo")

    def test_seeds_are_tagged_to_display_in_catalogue_and_subject_area(self):
        entity = self.results.entity(
            "urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e"
        )
        assert set(entity.tag_names) == {
            "urn:li:tag:dc_display_in_catalogue",
            "urn:li:tag:Courts and tribunals",
        }
