from abc import ABCMeta
import logging

from typing import List, Optional

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import ContainerTransformer
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass, 
    GlobalTagsClass,
    TagAssociationClass,
    ContainerPropertiesClass
)

# Used to apply ownership to containers
OWNERSHIP_TYPE = "DATAOWNER"

ENTITY_TYPES = ["container"]

logging.basicConfig(level=logging.DEBUG)

class AddOwnershipTransformerConfig(ConfigModel):
    semantics: str = "OVERWRITE"
    data_custodian: str
    ownership_type: str = OWNERSHIP_TYPE


class AddPropertiesTransformerConfig(ConfigModel):
    semantics: str = "OVERWRITE"
    description: str = ""
    properties: dict[str, str] = {}


class AddTagTransformerConfig(ConfigModel):
    semantics: str = "OVERWRITE"
    tag_urns: list[str]



class AddOwnershipTransformer(ContainerTransformer):
    """Transformer that adds owners to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddOwnershipTransformerConfig

    def __init__(self, config: AddOwnershipTransformerConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddOwnershipTransformer":
        config = AddOwnershipTransformerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return ENTITY_TYPES
    
    def aspect_name(self) -> str:
        return "ownership"
    
    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[OwnershipClass | None]:
        """Transform the Ownership aspect by adding configured owners.
        When the ingestion occurs owners will be added via the config, semantics control how they are added.

        Args:
            entity_urn (str): Urn of the container entity being ingested
            aspect_name (str): ownership
            aspect (Optional[OwnershipClass]): OwnershipClass being transformed coming from the ingestion pipeline

        Returns:
            Optional[OwnershipClass]: Transformed OwnershipClass with added owners
        """
        # Default behaviour is to overwrite existing owners
        if self.config.semantics == "OVERWRITE":
            # In the event that ownership configuration is missing, skip adding ownership
            if not self.config.data_custodian and not self.config.ownership_type:
                logging.warning(
                    f"No data custodian or ownership type provided, skipping ownership addition for {entity_urn}"
                )
                return aspect
            
            # Define new owner
            owner = OwnerClass(
                owner=self.config.data_custodian,
                type=self.config.ownership_type,
                source=None,
            )

            # Create new Ownership aspect with the new owner
            ownership = OwnershipClass(
                owners=[owner],
            )
            return ownership

class AddPropertiesTransformer(ContainerTransformer):
    """Transformer that adds configured properties to containers."""

    ctx: PipelineContext
    config: AddPropertiesTransformerConfig

    def __init__(self, config: AddPropertiesTransformerConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddPropertiesTransformer":
        config = AddPropertiesTransformerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return ENTITY_TYPES
    
    def aspect_name(self) -> str:
        return "containerProperties"

    
    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[ContainerPropertiesClass]
    ) -> Optional[ContainerPropertiesClass]:
        """Transform the ContainerProperties aspect by adding configured properties.
        When the ingestion occurs properties will be added via the config, semantics control how they are added.

        Args:
            entity_urn (str): Urn of the container entity being ingested
            aspect_name (str): containerProperties
            aspect (Optional[ContainerPropertiesClass]): ContainerPropertiesClass being transformed

        Returns:
            Optional[ContainerPropertiesClass]: Transformed ContainerPropertiesClass with added properties
        """
        # Currently handles description and custom properties only
        new_description = self.config.description or ""
        new_custom_properties = self.config.properties or {}
        
        # Default behaviour is to overwrite existing description and properties with options provided
        if self.config.semantics == "OVERWRITE":
            # If we have an existing aspect, overwrite the description and custom properties
            if aspect and isinstance(aspect, ContainerPropertiesClass):
                # If aspect exists, overwrite description and custom properties 
                if new_description:
                    aspect.description = new_description
                if new_custom_properties:
                    aspect.customProperties = new_custom_properties
        return aspect
    
class AddTagTransformer(ContainerTransformer):
    """Transformer that adds configured tags to containers."""

    ctx: PipelineContext
    config: AddTagTransformerConfig

    def __init__(self, config: AddTagTransformerConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddTagTransformer":
        config = AddTagTransformerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return ENTITY_TYPES
    
    def aspect_name(self) -> str:
        return "globalTags"
    
    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[GlobalTagsClass]:
        """Transform the GlobalTags aspect by adding configured tags.
        When the ingestion occurs tags will be added via the config, semantics control how they are added.

        Args:
            entity_urn (str): Urn of the container entity being ingested
            aspect_name (str): GlobalTagsClass
            aspect (Optional[Aspect]): GlobalTagsClass being transformed

        Returns:
            Optional[GlobalTagsClass]: List of GlobalTagsClass objects to be added to the container 
        """
        # Check provided tag urns, if none exist, return existing aspect
        if not self.config.tag_urns:
            logging.warning(
                f"No tag urns provided, skipping tag addition for {entity_urn}"
            )
            return aspect
        
        # Default behaviour is to overwrite existing tags, if provided.
        if self.config.semantics == "OVERWRITE":
            # Obtain the new tags to be added        
            new_tags = []
            for tag_urn in self.config.tag_urns:
                new_tags.append(TagAssociationClass(tag=tag_urn))
            
            # We are not interested in any pre-existing tags for OVERWRITE semantics
            return GlobalTagsClass(tags=new_tags)