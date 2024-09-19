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
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    CorpUserSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    CorpUserInfoClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    TagAssociationClass,
)

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.create_cadet_databases_source.config import CreateCadetDatabasesConfig
from ingestion.ingestion_utils import (
    format_domain_name,
    get_cadet_metadata_json,
    get_tags,
    parse_database_and_table_names,
    validate_fqn,
)
from ingestion.utils import report_generator_time, report_time

logging.basicConfig(level=logging.DEBUG)


@config_class(CreateCadetDatabasesConfig)
class CreateCadetDatabases(Source):
    source_config: CreateCadetDatabasesConfig
    report: SourceReport = SourceReport()

    @report_time
    def __init__(self, config: CreateCadetDatabasesConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = CreateCadetDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @report_generator_time
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        manifest = get_cadet_metadata_json(self.source_config.manifest_s3_uri)
        databases_metadata = get_cadet_metadata_json(
            self.source_config.database_metadata_s3_uri
        )

        # Create all the domain entities
        for domain_name in self._get_domains(manifest):
            mcp = self._make_domain(domain_name)
            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            self.report.report_workunit(wu)
            logging.info(f"Creating domain {domain_name}")

            yield wu

        # Create database entities and assign them to their domains
        databases_with_metadata, tables_with_domains, display_tags = (
            self._get_databases_with_domains_and_display_tags(
                manifest, databases_metadata
            )
        )
        sub_types: list[str] = [DatasetContainerSubTypes.DATABASE]
        last_modified = int(datetime.now().timestamp())

        # make database owners as users in datahub
        for _, db_meta_tuple in databases_with_metadata:
            db_meta_dict = dict(db_meta_tuple)
            if not db_meta_dict.get("dc_owner", "") == "":
                mce = self._make_user(db_meta_dict["dc_owner"])
                wu = MetadataWorkUnit("single_mce", mce=mce)
                self.report.report_workunit(wu)
                yield wu

        for database_name, database_metadata in databases_with_metadata:
            database_container_key = mcp_builder.DatabaseKey(
                database=database_name,
                platform=PLATFORM,
                instance=INSTANCE,
                env=ENV,
                backcompat_env_as_instance=True,
            )
            db_meta_dict = dict(database_metadata)
            domain_name = format_domain_name(db_meta_dict["domain"])
            domain_urn = mce_builder.make_domain_urn(domain=domain_name)
            display_tag = display_tags.get(database_name)

            if not db_meta_dict.get("dc_owner", "") == "":
                owner_urn = mce_builder.make_user_urn(
                    db_meta_dict.pop("dc_owner").split("@")[0]
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
                domain_urn=domain_urn,
                external_url=None,
                description=database_description,
                created=None,
                last_modified=last_modified,
                tags=display_tag,
                owner_urn=owner_urn,
                qualified_name=None,
                extra_properties=db_meta_dict,
            )

        # Add dc_display_in_catalogue tag to all seeds
        seed_nodes = [
            manifest["nodes"][node]
            for node in manifest["nodes"]
            if manifest["nodes"][node]["resource_type"] == "seed"
        ]
        tag_to_add = mce_builder.make_tag_urn("dc_display_in_catalogue")
        tag_association_to_add = TagAssociationClass(tag=tag_to_add)
        current_tags = GlobalTagsClass(tags=[tag_association_to_add])
        for node in seed_nodes:
            database, table = parse_database_and_table_names(node)
            dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                platform=PLATFORM,
                name=f"{database}.{table}",
                platform_instance=INSTANCE,
            )
            mcp: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=current_tags,
            )
            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            self.report.report_workunit(wu)
            logging.info(
                f"Tagging seed {database}.{table} with dc_display_in_catalogue"
            )
            yield wu

        # Assign domains to tables
        for database, table, domain in tables_with_domains:
            dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                platform=PLATFORM,
                name=f"{database}.{table}",
                platform_instance=INSTANCE,
            )
            domain_name = format_domain_name(domain)
            domain_urn = mce_builder.make_domain_urn(domain=domain_name)
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspect=DomainsClass(domains=[domain_urn]),
            )

            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            self.report.report_workunit(wu)
            logging.info(f"Assigning {domain_name} domain to {database}.{table}")
            yield wu

    def _get_domains(self, manifest) -> set[str]:
        """Only models are arranged by domain in CaDeT.
        Seeds should only be associated with a domain if it appears in models.
        """
        return set(
            format_domain_name(manifest["nodes"][node]["fqn"][1])
            for node in manifest["nodes"]
            if manifest["nodes"][node]["resource_type"] == "model"
        )

    def _make_user(self, email: str):
        if not email.endswith(".gov.uk"):
            email = email + "@justice.gov.uk"
        user_urn = mce_builder.make_user_urn(email.split("@")[0])
        user_snapshot = CorpUserSnapshot(
            urn=user_urn,
            aspects=[Status(removed=False)],
        )
        user_info = CorpUserInfoClass(
            active=False,
            displayName=email.split("@")[0].replace(".", " "),
            email=email,
        )
        user_snapshot.aspects.append(user_info)
        user_mce = MetadataChangeEvent(proposedSnapshot=user_snapshot)
        return user_mce

    @report_time
    def _get_databases_with_domains_and_display_tags(
        self, manifest: dict, databases_metadata: dict
    ) -> tuple[set[tuple[str, tuple]], set[tuple[str, str, str]], dict]:
        """
        These mappings will only work with tables named {database}__{table}
        like create a derived table.

        returns a set of databases with associated domain and a dict for
        display tags, where key is database and value is dc_display_in_catalogue
        if any model is to be displayed
        """
        database_mappings = set()
        table_mappings = set()
        tag_mappings = {}
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] in ["model", "seed"]:
                # fqn = fully qualified name
                fqn = manifest["nodes"][node]["fqn"]
                if validate_fqn(fqn):
                    database, table = parse_database_and_table_names(
                        manifest["nodes"][node]
                    )
                    database_metadata_dict = {}
                    tag = (
                        "dc_display_in_catalogue"
                        if "dc_display_in_catalogue" in manifest["nodes"][node]["tags"]
                        else None
                    )

                    if tag == "dc_display_in_catalogue":
                        try:
                            database_metadata_dict = databases_metadata["databases"][
                                database
                            ]
                        except KeyError:
                            logging.warning(
                                f"{database} - has no database level metadata"
                            )

                    database_metadata_dict["domain"] = fqn[1]
                    database_metadata_tuple = tuple(database_metadata_dict.items())
                    database_mappings.add((database, database_metadata_tuple))
                    table_mappings.add(
                        (database, table, database_metadata_dict["domain"])
                    )

                    database, table = parse_database_and_table_names(
                        manifest["nodes"][node]
                    )
                    domain = fqn[1]
                    database_mappings.add((database, database_metadata_tuple))
                    table_mappings.add(
                        (database, table, database_metadata_dict["domain"])
                    )

                    tags = get_tags(manifest["nodes"][node])
                    if tags:
                        tag_mappings[database] = tags

        return database_mappings, table_mappings, tag_mappings

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

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        logging.info("Completed ingestion")
