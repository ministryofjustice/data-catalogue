import logging
from abc import ABCMeta
import os
from pathlib import Path
from typing import List, Optional, Union

from datahub.configuration.common import ConfigModel
from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import ContainerTransformer
from datahub.metadata._schema_classes import (
    ContainerPropertiesClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    TagAssociationClass,
)
from datahub.metadata.schema_classes import MetadataChangeProposalClass

from ingestion.utils import report_time

URN_CONTAINER_PREFIX = "urn:li:container:"
DATAOWNER = "DATAOWNER"

logging.basicConfig(level=logging.DEBUG)


class EnrichContainerTransformerConfig(ConfigModel):
    data_custodian: str
    subject_areas: list[str]
    ownership_type: str = DATAOWNER
    description: str = ""
    description_map: dict[str, str] = {}
    properties: dict[str, str] = {}


class EnrichContainerTransformer(ContainerTransformer, metaclass=ABCMeta):
    """Transformer that adds an owner and tags for a provided container"""

    ctx: PipelineContext
    config: EnrichContainerTransformerConfig

    def __init__(self, config: EnrichContainerTransformerConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "EnrichContainerTransformer":
        config = EnrichContainerTransformerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def aspect_name(self):
        return "container"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        return None

    @report_time
    def handle_end_of_stream(
        self,
    ) -> List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:

        logging.debug("Generating Ownership for containers")
        logging.debug(f"{self.config.subject_areas=}, {self.config.data_custodian=}")

        mcps: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = []

        # All containers need the catalogue tag
        tag_to_add = mce_builder.make_tag_urn("dc_display_in_catalogue")
        tag_association_to_add = TagAssociationClass(tag=tag_to_add)
        subject_area_tags = [
            TagAssociationClass(tag=mce_builder.make_tag_urn(subject_area))
            for subject_area in self.config.subject_areas
        ]
        current_tags = GlobalTagsClass(
            tags=[tag_association_to_add, *subject_area_tags]
        )

        owner_to_add = OwnerClass(
            self.config.data_custodian, self.config.ownership_type
        )

        for container_urn in self.entity_map.keys():
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=container_urn,
                    aspect=OwnershipClass(owners=[owner_to_add]),
                )
            )
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=container_urn,
                    aspect=current_tags,
                )
            )

            if self.config.properties:
                mcps.append(self.properties_mcp(container_urn))

        return mcps

    def properties_mcp(self, container_urn):
        if not self.ctx.graph:
            raise Exception("Graph not available")

        current_properties = self.ctx.graph.get_aspect(
            entity_urn=container_urn, aspect_type=ContainerPropertiesClass
        )
        if not current_properties:
            raise Exception("Unable to query current properties")

        if self.config.description_map:
            # Use the description map to set the description if available
            description_file = self.config.description_map.get(current_properties.name)
            if description_file:
                try:
                    path = os.path.join(
                        Path(__file__).parent.absolute(),
                        "descriptions",
                        f"{description_file}.txt",
                    )
                    with open(path, "r", encoding="utf-8") as file:
                        description = file.read().strip()
                        current_properties.description = description
                except FileNotFoundError:
                    logging.warning(
                        f"Description file {description_file} not found for container {current_properties.name}"
                    )
        elif self.config.description:
            current_properties.description = self.config.description

        current_properties.customProperties.update(self.config.properties)
        return MetadataChangeProposalWrapper(
            entityUrn=container_urn,
            aspect=current_properties,
        )
