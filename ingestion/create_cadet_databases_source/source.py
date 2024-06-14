import logging
from datetime import datetime
from typing import Iterable

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.metadata.schema_classes import ChangeTypeClass, DomainPropertiesClass

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.create_cadet_databases_source.config import CreateCadetDatabasesConfig
from ingestion.dbt_manifest_utils import get_cadet_manifest, validate_fqn


@config_class(CreateCadetDatabasesConfig)
class CreateCadetDatabases(Source):
    source_config: CreateCadetDatabasesConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: CreateCadetDatabasesConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = CreateCadetDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        manifest = get_cadet_manifest(self.source_config.manifest_s3_uri)

        # Create all the domain entities
        for domain_name in self._get_domains(manifest):
            mcp = self._make_domain(domain_name)
            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            self.report.report_workunit(wu)
            logging.info(f"Creating domain {domain_name}")

            yield wu

        # Create database entities and assign them to their domains
        databases_with_domains, display_tags = (
            self._get_databases_with_domains_and_display_tags(manifest)
        )
        sub_types = [DatasetContainerSubTypes.DATABASE]
        last_modified = int(datetime.now().timestamp())
        for database, domain in databases_with_domains:
            database_container_key = mcp_builder.DatabaseKey(
                database=database,
                platform=PLATFORM,
                instance=INSTANCE,
                env=ENV,
                backcompat_env_as_instance=True,
            )
            domain_urn = mce_builder.make_domain_urn(domain=domain)
            display_tag = display_tags.get(database)

            logging.info(f"Creating container {database=} with {domain=}")
            yield from mcp_builder.gen_containers(
                container_key=database_container_key,
                name=database,
                sub_types=sub_types,
                domain_urn=domain_urn,
                external_url=None,
                description=None,
                created=None,
                last_modified=last_modified,
                tags=[display_tag] if display_tag is not None else None,
                owner_urn=None,
                qualified_name=None,
                extra_properties=None,
            )

    def _get_domains(self, manifest) -> set[str]:
        """Only models are arranged by domain in CaDeT"""
        return set(
            manifest["nodes"][node]["fqn"][1]
            for node in manifest["nodes"]
            if manifest["nodes"][node]["resource_type"] == "model"
        )

    def _get_databases_with_domains_and_display_tags(
        self, manifest
    ) -> tuple[set[tuple[str, str]], dict]:
        """
        These mappings will only work with tables named {database}__{table}
        like create a derived table.

        returns a set of databases with associated domain and a dict for
        display tags, where key is database and value is dc_display_in_catalogue
        if any model is to be displayed
        """
        mappings = set()
        tags = {}
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] == "model":
                fqn = manifest["nodes"][node]["fqn"]
                if validate_fqn(fqn):
                    database = fqn[-1].split("__")[0]
                    domain = fqn[1]
                    tag = (
                        "dc_display_in_catalogue"
                        if "dc_display_in_catalogue" in manifest["nodes"][node]["tags"]
                        else None
                    )
                    mappings.add((database, domain))
                    if tag is not None:
                        tags[database] = tag

        return mappings, tags

    def _make_domain(self, domain_name) -> MetadataChangeProposalWrapper:
        domain_urn = mce_builder.make_domain_urn(domain=domain_name)
        domain_properties = DomainPropertiesClass(name=domain_name)
        metadata_event = MetadataChangeProposalWrapper(
            entityType="domain",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=domain_urn,
            aspect=domain_properties,
        )
        return metadata_event

    def _get_display_in_catalogue_tag(self, database):
        pass

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        logging.info("Completed ingestion")
