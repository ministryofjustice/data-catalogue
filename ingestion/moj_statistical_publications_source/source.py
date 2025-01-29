import datetime
import logging
from io import BufferedReader
from typing import Any, Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import TimeStamp
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    SubTypesClass,
    TagAssociationClass,
)
from datahub.utilities.time import datetime_to_ts_millis

from ingestion.ingestion_utils import FindMojDataEntityTypes, format_domain_name

from .api_client import MojPublicationsAPIClient
from .config import MojPublicationsAPIConfig

logging.basicConfig(level=logging.DEBUG)


@platform_name("File")
@config_class(MojPublicationsAPIConfig)
@support_status(SupportStatus.CERTIFIED)
class MojPublicationsAPISource(StatefulIngestionSourceBase):
    """
    This plugin pulls metadata from the gov.uk publications search and content APIs
    """

    def __init__(
        self,
        ctx: PipelineContext,
        config: MojPublicationsAPIConfig,
        validate_domains: bool = True,
    ) -> None:
        super().__init__(config, ctx)

        self.ctx = ctx
        self.config = config
        self.report = StaleEntityRemovalSourceReport()
        self.fp: Optional[BufferedReader] = None
        self.client = MojPublicationsAPIClient(
            config.base_url, config.default_contact_email, config.params
        )
        if validate_domains:
            self.client.validate_domains()
        self._id_to_domain_contact_mapping = self.client._id_to_domain_contact_mapping
        self.platform_name = "GOV.UK"
        self.platform_instance = "ministry-of-justice-publications"
        self.access_requirements = config.access_requirements
        self.web_url = self.config.base_url.removesuffix("/api").removesuffix("/api/")

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, ctx
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = MojPublicationsAPIConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self):
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        all_publications_metadata = self.client.list_all_publications_metadata()
        collections_metadata = self.client.get_collections_from_all_results(
            all_publications_metadata, self.config.collections_to_exclude
        )

        # create publication collections entities DatasetContainerSubTypes.FOLDER
        yield from self._create_publication_collections_containers(collections_metadata)

        mcps = self._make_publication_dataset_mcps(all_publications_metadata)
        for mcp in mcps:
            logging.info(f"creating {mcp.aspectName} for {mcp.entityUrn}")
            wu = MetadataWorkUnit(f"{mcp.entityUrn}-{mcp.aspectName}", mcp=mcp)

            yield wu

    def get_report(self):
        return self.report

    def _create_publication_collections_containers(
        self, collections_metadata: List[Dict[str, Any]]
    ) -> Generator[MetadataWorkUnit, Any, None]:
        sub_types: list[str] = [FindMojDataEntityTypes.PUBLICATION_COLLECTION]
        custom_properties: dict = {
            "dc_access_requirements": self.config.access_requirements,
            "security_classification": "Official",
        }
        for collection in collections_metadata:
            last_modified_date = datetime.datetime.fromisoformat(
                collection.get("last_updated", "")
            )

            custom_properties["dc_team_email"] = collection.get(
                "contact_email", self.client.default_contact_email
            )

            container_key = mcp_builder.DatabaseKey(
                database=collection.get("title", ""),
                platform=self.platform_name,
                instance=self.platform_instance,
                env="prod",
                backcompat_env_as_instance=True,
            )
            tags = ["dc_display_in_catalogue"]
            if collection.get("domain"):
                domain_name = format_domain_name(collection["domain"])
                domain_urn = mce_builder.make_domain_urn(domain=domain_name)
                tags.append(domain_name)
            else:
                domain_urn = None

            collection_name = collection.get("title", "")
            logging.info(f"Creating container for {collection_name=}")
            yield from mcp_builder.gen_containers(
                container_key=container_key,
                name=collection_name,
                sub_types=sub_types,
                domain_urn=domain_urn,
                external_url=urljoin(self.client.base_url, collection.get("link")),
                description=collection.get("description"),
                created=None,
                last_modified=datetime_to_ts_millis(last_modified_date),
                tags=tags,
                owner_urn=None,
                qualified_name=collection.get("slug"),
                extra_properties=custom_properties,
            )

    def _make_publication_dataset_mcps(
        self, all_publications_metadata: List[Dict]
    ) -> list[MetadataChangeProposalWrapper]:
        """
        creates the aspects for dataset properties, tags, domain, and container, for
        all individual publcations as a dataset and returns as a list of mcps

        All publications will not have a container or domain (if not in a collection)
        """
        mcps = []
        custom_properties: dict = {
            "dc_access_requirements": self.config.access_requirements,
            "security_classification": "Official",
        }
        sub_types: list[str] = [FindMojDataEntityTypes.PUBLICATION_DATASET]
        for publication in all_publications_metadata:

            dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                self.platform_name,
                publication["_id"].split("/")[-1],
                self.platform_instance,
            )

            last_modified_date = datetime.datetime.fromisoformat(
                publication["public_timestamp"]
            )

            # if publication is in a collection it's special and gets some more metadata we've collected in
            # a mapping yaml. Namely a domain and team contact email where available
            if publication.get("document_collections"):

                # publications can belong to multiple collections - opting to keep it more simple and register
                # a publication against the first collection from the api.

                # We'd need to explore a different approach to include multiple collection association

                # There are 133 in mulitple collections at time of writing, about 10%
                parent_collection_titles = [
                    dc["title"] for dc in publication["document_collections"]
                ]
                parent_collection_ids = [
                    dc["slug"] for dc in publication["document_collections"]
                ]

                container_key = mcp_builder.DatabaseKey(
                    database=parent_collection_titles[0],
                    platform=self.platform_name,
                    instance=self.platform_instance,
                    env="prod",
                    backcompat_env_as_instance=True,
                )

                parent_collection_urn = mce_builder.make_container_urn(container_key)
                custom_properties.update(
                    {
                        "dc_team_email": self._id_to_domain_contact_mapping.get(
                            parent_collection_ids[0], {}
                        ).get("contact_email", self.client.default_contact_email)
                    }
                )

                # assign publication to its collection
                mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=ContainerClass(container=parent_collection_urn),
                    )
                )
                domain = self._id_to_domain_contact_mapping.get(
                    parent_collection_ids[0], {}
                ).get("domain")

                # because as is there won't always be an applicable domain (subject area)
                # even within a colleciton
                tags = [TagAssociationClass(tag="urn:li:tag:dc_display_in_catalogue")]
                if domain:
                    domain_name = format_domain_name(domain)
                    tags.append(TagAssociationClass(tag=f"urn:li:tag:{domain_name}"))
                    domain_urn = mce_builder.make_domain_urn(domain=domain_name)
                else:
                    domain_urn = None

                # add domain - we only add a domain for publications within a collection
                if domain_urn:
                    mcps.append(
                        MetadataChangeProposalWrapper(
                            entityUrn=dataset_urn,
                            aspect=DomainsClass(domains=[domain_urn]),
                        )
                    )
            else:
                custom_properties["dc_team_email"] = self.client.default_contact_email

            # add dataset properties
            publication_properties = DatasetPropertiesClass(
                description=publication.get("description") or "",
                name=publication["title"],
                lastModified=TimeStamp(time=datetime_to_ts_millis(last_modified_date)),
                externalUrl=urljoin(self.client.base_url, publication["link"]),
                customProperties=custom_properties,
                qualifiedName=publication["_id"],
            )

            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=publication_properties,
                )
            )

            # add subtype to all datasets
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=SubTypesClass(typeNames=sub_types),
                )
            )

            # add tag to display to all datasets
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(tags=tags),
                )
            )

            # add platform instance to all datasets
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=DataPlatformInstanceClass(
                        platform=mce_builder.make_data_platform_urn(self.platform_name),
                        instance=mce_builder.make_dataplatform_instance_urn(
                            self.platform_name, self.platform_instance
                        ),
                    ),
                )
            )

        return mcps

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(
                capable=False,
                failure_reason=f"Haven't implemented this yet!",
            )
        )
