from collections import defaultdict

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from utils import group_metadata

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
        self.results = group_metadata(source.get_workunits())

    def test_creating_domains_from_s3(self):
        urns = [
            "urn:li:container:27c5c4df57bf429bf9e56e51b30003ed",
            "urn:li:container:ea9744b8004d93b716687bab12438c90",
            "urn:li:container:48e5e41ce461da41f0333b67a322fb99",
            "urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e",
            "urn:li:container:b17e173b8950dee2415a3119fb7c9d12",
        ]

        for urn in urns:
            assert urn in self.results
            aspects = self.results[urn]

            # Events are created for the following aspects per database:
            # create container, update status, add platform, add subtype, associate container with domain, add tags
            container_events = aspects["containerProperties"]
            status_events = aspects["status"]
            platform_events = aspects["dataPlatformInstance"]
            sub_types_events = aspects["subTypes"]
            tags_events = aspects["globalTags"]

        assert (
            len(container_events) == len(sub_types_events) == len(platform_events) == 5
        )

        assert len(tags_events) == 6

        assert container_events[0].metadata.aspect.customProperties.get("database")

            assert (platform_events[0].platform) == builder.make_data_platform_urn(
                platform="dbt"
            )
            assert DatasetContainerSubTypes.DATABASE in sub_types_events[0].typeNames

        assert self.results.get("urn:li:corpuser:some.one", {}).get("corpUserInfo")
        assert self.results.get("urn:li:corpuser:some.team", {}).get("corpUserInfo")

    def test_seeds_are_tagged_to_display_in_catalogue_and_subject_area(self):
        tag_names = [
            tag.tag
            for tagAspect in self.results[
                "urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e"
            ]["globalTags"]
            for tag in tagAspect.tags
        ]
        assert set(tag_names) == {
            "urn:li:tag:dc_display_in_catalogue",
            "urn:li:tag:Courts and tribunals",
        }
