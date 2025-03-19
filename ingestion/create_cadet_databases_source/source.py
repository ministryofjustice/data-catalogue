import logging
from datetime import datetime
from typing import Iterable, List, Optional

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.create_cadet_databases_source.config import CreateCadetDatabasesConfig
from ingestion.ingestion_utils import (
    NodeLookup,
    domains_to_subject_areas,
    get_cadet_metadata_json,
    get_subject_areas,
    get_tags,
    make_user_mcp,
    parse_database_and_table_names,
    validate_fqn,
)
from ingestion.utils import report_generator_time, report_time

logging.basicConfig(level=logging.DEBUG)

properties_to_add = {
    "security_classification": "Official-Sensitive",
}


@config_class(CreateCadetDatabasesConfig)
class CreateCadetDatabases(StatefulIngestionSourceBase):

    @report_time
    def __init__(self, config: CreateCadetDatabasesConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report = StaleEntityRemovalSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = CreateCadetDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    @report_generator_time
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        manifest = get_cadet_metadata_json(self.source_config.manifest_s3_uri)
        databases_metadata = get_cadet_metadata_json(
            self.source_config.database_metadata_s3_uri
        )

        mcps: list[MetadataChangeProposalWrapper] = []

        # Get database metadata from the manifest and database metadata dicts
        databases_with_metadata, domain_lookup, display_tags = (
            self._get_databases_with_domains_and_display_tags(
                manifest, databases_metadata
            )
        )

        # create mcps for database owner corpusers
        mcps.extend(self.create_database_owner_mcps(databases_with_metadata))

        # create mcps to tag seed datasets with dc_display_in_catalogue
        mcps.extend(self.create_display_tag_for_seed_mcps(manifest, domain_lookup))

        # create the cadet databases tagged to display
        yield from self.create_database_mcps(databases_with_metadata, display_tags)

        for mcp in mcps:
            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            logging.info(f"creating {wu.metadata.aspect} for {wu.metadata.entityUrn}")
            yield wu

    def create_database_owner_mcps(
        self, databases_with_metadata: set
    ) -> list[MetadataChangeProposalWrapper]:
        database_owner_mcps = []
        for _, db_meta_tuple in databases_with_metadata:
            db_meta_dict = dict(db_meta_tuple)
            if not db_meta_dict.get("dc_data_custodian", "") == "":
                mcp = make_user_mcp(db_meta_dict["dc_data_custodian"])
                database_owner_mcps.append(mcp)

        return database_owner_mcps

    def create_database_mcps(
        self, databases_with_metadata, display_tags
    ) -> Iterable[MetadataWorkUnit]:
        sub_types: list[str] = [DatasetContainerSubTypes.DATABASE]
        last_modified = int(datetime.now().timestamp())

        for database_name, database_metadata in databases_with_metadata:
            database_container_key = mcp_builder.DatabaseKey(
                database=database_name,
                platform=PLATFORM,
                instance=INSTANCE,
                env=ENV,
                backcompat_env_as_instance=True,
            )
            db_meta_dict = dict(database_metadata)
            db_meta_dict.update(properties_to_add)
            domain_name = db_meta_dict["domain"]
            tags = set(display_tags.get(database_name, ["dc_cadet"]))
            if domains_to_subject_areas.get(domain_name.lower()):
                tags.add(domains_to_subject_areas[domain_name.lower()])

            if not db_meta_dict.get("", "") == "":
                owner_urn = mce_builder.make_user_urn(
                    db_meta_dict.pop("dc_data_custodian").split("@")[0]
                )
            else:
                owner_urn = None

            if not db_meta_dict.get("description", "") == "":
                database_description = db_meta_dict.pop("description")
            else:
                database_description = None

            logging.info(f"Creating container {database_name=} with {domain_name=}")
            yield from mcp_builder.gen_containers(
                container_key=database_container_key,
                name=database_name,
                sub_types=sub_types,
                external_url=None,
                description=database_description,
                created=None,
                last_modified=last_modified,
                tags=tags,
                owner_urn=owner_urn,
                qualified_name=None,
                extra_properties=db_meta_dict,
            )

    def create_display_tag_for_seed_mcps(
        self, manifest, domain_lookup
    ) -> list[MetadataChangeProposalWrapper]:
        seed_domain_mcps = []

        seed_nodes = [
            manifest["nodes"][node]
            for node in manifest["nodes"]
            if manifest["nodes"][node]["resource_type"] == "seed"
        ]
        for node in seed_nodes:
            database, table = parse_database_and_table_names(node)
            domain = domain_lookup.get(database, table)
            tag_names = [
                "Miscellaneous",
                "Reference data",
                "dc_display_in_catalogue",
            ]
            if domains_to_subject_areas.get(domain.lower()):
                tag_names.append(domains_to_subject_areas.get(domain.lower()))

            tags_aspect = GlobalTagsClass(
                tags=[
                    TagAssociationClass(tag=mce_builder.make_tag_urn(tag_name))
                    for tag_name in tag_names
                ]
            )

            dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                platform=PLATFORM,
                name=f"{database}.{table}",
                platform_instance=INSTANCE,
            )
            mcp: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=tags_aspect,
            )

            seed_domain_mcps.append(mcp)
        return seed_domain_mcps

    @report_time
    def _get_databases_with_domains_and_display_tags(
        self, manifest: dict, databases_metadata: dict
    ) -> tuple[set[tuple[str, tuple[str, str]]], NodeLookup[str], dict[str, list[str]]]:
        """
        These mappings will only work with tables named {database}__{table}
        like create a derived table.

        Returns:
            - database_mappings: a set of databases with associated metadata
            - domain_lookup: a domain lookup for tables and databases
            - tag_mappings: a dict for display tags, where key is database and
            value is the desired tags, including dc_display_in_catalogue if
            any model is to be displayed.
        """
        database_mappings = set()
        domain_lookup = NodeLookup()
        tag_mappings = {}
        top_level_subject_areas = get_subject_areas()
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] in ["model", "seed"]:
                # fqn = fully qualified name
                fqn = manifest["nodes"][node]["fqn"]
                if validate_fqn(fqn):
                    database, table = parse_database_and_table_names(
                        manifest["nodes"][node]
                    )
                    database_metadata_dict = {}

                    try:
                        database_metadata_dict = databases_metadata["databases"][
                            database
                        ].copy()
                    except KeyError:
                        logging.debug(f"{database} - has no database level metadata")

                    database_metadata_dict["domain"] = fqn[1]
                    database_tags = database_metadata_dict.get("tags", [])
                    if "tags" in database_metadata_dict:
                        database_metadata_dict.pop("tags")
                    database_metadata_tuple = tuple(database_metadata_dict.items())
                    database_mappings.add((database, database_metadata_tuple))
                    domain_lookup.set(database, table, database_metadata_dict["domain"])

                    tags = get_tags(manifest["nodes"][node])
                    if database_tags:
                        tags.update(database_tags)
                    if not any(tag in top_level_subject_areas for tag in tags):
                        logging.warning(
                            f"No top level tags found in database metadata file for {database}"
                        )

                    if tags:
                        if tag_mappings.get(database):
                            tag_mappings[database].update(tags)
                        else:
                            tag_mappings[database] = tags

        return database_mappings, domain_lookup, tag_mappings

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        logging.info("Completed ingestion")
