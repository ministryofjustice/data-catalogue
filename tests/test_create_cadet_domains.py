from collections import defaultdict

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    CorpUserInfoClass,
    DataPlatformInstanceClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    StatusClass,
    SubTypesClass,
)

from ingestion.create_cadet_databases_source.config import CreateCadetDatabasesConfig
from ingestion.create_cadet_databases_source.source import CreateCadetDatabases
from ingestion.ingestion_utils import format_domain_name


class TestCreateCadetDatabases:
    def setup_method(self):
        source = CreateCadetDatabases(
            ctx=PipelineContext(run_id="domain-source-test"),
            config=CreateCadetDatabasesConfig(
                manifest_s3_uri="s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
                database_metadata_s3_uri="s3://test_bucket/prod/run_artefacts/latest/target/database_metadata.json",
            ),
        )
        self.results = list(source.get_workunits())
        self.results.sort(key=lambda event: event.metadata.entityUrn)

        self.results_by_aspect_type = defaultdict(list)
        for result in self.results:
            aspect_type = type(result.metadata.aspect)
            self.results_by_aspect_type[aspect_type].append(result)

    def test_creating_domains_from_s3(self):
        domain_creation_events = self.results_by_aspect_type[DomainPropertiesClass]
        domains = [event.metadata.aspect.name for event in domain_creation_events]
        assert domains == ["Courts", "HQ", "Prison", "Probation"]

        # Events are created for the following aspects per database:
        # create container, update status, add platform, add subtype, associate container with domain, add tags
        container_events = self.results_by_aspect_type[ContainerPropertiesClass]
        status_events = self.results_by_aspect_type[StatusClass]
        platform_events = self.results_by_aspect_type[DataPlatformInstanceClass]
        sub_types_events = self.results_by_aspect_type[SubTypesClass]
        domains_events = self.results_by_aspect_type[DomainsClass]
        tags_events = self.results_by_aspect_type[GlobalTagsClass]
        user_creation_events = self.results_by_aspect_type[CorpUserInfoClass]

        assert (
            len(container_events) == len(sub_types_events) == len(platform_events) == 5
        )

        assert len(domains_events) == 10

        assert len(tags_events) == 6

        assert container_events[0].metadata.aspect.customProperties.get("database")

        assert (
            platform_events[0].metadata.aspect.platform
        ) == builder.make_data_platform_urn(platform="dbt")
        assert (
            DatasetContainerSubTypes.DATABASE
            in sub_types_events[0].metadata.aspect.typeNames
        )
        assert (
            container_events[0].metadata.entityUrn
            == domains_events[0].metadata.entityUrn
        )
        expected_domain = (
            container_events[0]
            .metadata.aspect.customProperties.get("database")
            .split("_")[0]
        )
        assert (
            builder.make_domain_urn(format_domain_name(expected_domain))
            in domains_events[0].metadata.aspect.domains
        )

        user_urns = [event.metadata.entityUrn for event in user_creation_events]
        assert user_urns == ["urn:li:corpuser:some.one", "urn:li:corpuser:some.team"]

    def test_seeds_are_tagged_to_display_in_catalogue(self):
        seed_tag_event = [
            result
            for result in self.results_by_aspect_type[GlobalTagsClass]
            if result.metadata.entityType == "dataset"
        ]
        assert seed_tag_event[0].metadata.entityType == "dataset"
        assert seed_tag_event[0].metadata.changeType == "UPSERT"
        tag_names = {tag.tag for tag in seed_tag_event[0].metadata.aspect.tags}
        assert "urn:li:tag:dc_display_in_catalogue" in tag_names

    def test_datasets_are_assigned_to_domains(self):
        # This is the first event which should associate a dataset with a domain
        dataset_with_domains = [
            result
            for result in self.results_by_aspect_type[DomainsClass]
            if result.metadata.entityType == "dataset"
        ]
        assert dataset_with_domains[0].metadata.entityType == "dataset"
        assert dataset_with_domains[0].metadata.changeType == "UPSERT"
        assert dataset_with_domains[0].metadata.aspect.ASPECT_NAME == "domains"
