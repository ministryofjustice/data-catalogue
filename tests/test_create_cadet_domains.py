from collections import defaultdict

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
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

        # test for user mce
        user_creation_events = self.results[4:6]
        user_urns = [
            event.metadata.proposedSnapshot.urn for event in user_creation_events
        ]
        user_urns.sort()
        assert user_urns == ["urn:li:corpuser:some.one", "urn:li:corpuser:some.team"]

        # Events are created for the following aspects per database:
        # create container, update status, add platform, add subtype, associate container with domain, add tags
        container_events = self.results_by_aspect_type[ContainerPropertiesClass]
        status_events = self.results_by_aspect_type[StatusClass]
        platform_events = self.results_by_aspect_type[DataPlatformInstanceClass]
        sub_types_events = self.results_by_aspect_type[SubTypesClass]
        domains_events = self.results_by_aspect_type[DomainsClass]
        tags_events = self.results_by_aspect_type[GlobalTagsClass]

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

    def test_seeds_are_tagged_to_display_in_catalogue(self):
        seed_tag_event = self.results[34]
        assert seed_tag_event.metadata.entityType == "dataset"
        assert seed_tag_event.metadata.changeType == "UPSERT"
        assert (
            seed_tag_event.metadata.aspect.tags[0].tag
            == "urn:li:tag:dc_display_in_catalogue"
        )

    def test_datasets_are_assigned_to_domains(self):
        # This is the first event which should associate a dataset with a domain
        assert self.results[35].metadata.entityType == "dataset"
        assert self.results[35].metadata.changeType == "UPSERT"
        assert self.results[35].metadata.aspect.ASPECT_NAME == "domains"
