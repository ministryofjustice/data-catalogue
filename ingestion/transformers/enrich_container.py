import logging

from typing import List, Optional

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
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
    data_custodian: str
    ownership_type: str = OWNERSHIP_TYPE


class AddPropertiesTransformerConfig(ConfigModel):
    description: str = ""
    properties: dict[str, str] = {}


class AddTagTransformerConfig(ConfigModel):
    tag_urns: list[str]



class AddOwnershipTransformer(BaseTransformer, SingleAspectTransformer):
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
    ) -> Optional[OwnershipClass]:
        """Transform the Ownership aspect by adding configured owners.
        Important: Currently does not obtain or remove existing owners. Existing owners would need to be,
        obtained via the ctx graph if required. 
        When the ingestion occurs owners will be added via the config, semantics control how they are added.

        Args:
            entity_urn (str): Urn of the container entity being ingested
            aspect_name (str): ownership
            aspect (Optional[OwnershipClass]): OwnershipClass being transformed

        Returns:
            Optional[OwnershipClass]: Transformed OwnershipClass with added owners
        """
        
        assert aspect is None or isinstance(aspect, OwnershipClass)

        if not self.config.data_custodian and not self.config.ownership_type:
            logging.warning(
                f"No data custodian or ownership type provided, skipping ownership addition for {entity_urn}"
            )
            return aspect
        
        owner = OwnerClass(
            owner=self.config.data_custodian,
            type=self.config.ownership_type,
            source=None,
        )
        
        ownership = (
            aspect
            if aspect
            else OwnershipClass(
                owners=[],
            )
        )
        ownership.owners.append(owner)
        return ownership

class AddPropertiesTransformer(BaseTransformer, SingleAspectTransformer):
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
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[ContainerPropertiesClass]:
        """Transform the ContainerProperties aspect by adding configured properties.
        Important: Currently does not obtain or remove existing properties. Existing properties would need to be,
        obtained via the ctx graph if required. 
        When the ingestion occurs properties will be added via the config, semantics control how they are added.

        Args:
            entity_urn (str): Urn of the container entity being ingested
            aspect_name (str): containerProperties
            aspect (Optional[Aspect]): ContainerPropertiesClass being transformed

        Returns:
            Optional[ContainerPropertiesClass]: Transformed ContainerPropertiesClass with added properties
        """
        
        assert aspect is None or isinstance(aspect, ContainerPropertiesClass)
        new_description = self.config.description
        new_custom_properties = self.config.properties

        if not new_description and not new_custom_properties:
            logging.warning(
                f"No description or custom properties provided, skipping property addition for {entity_urn}"
            )
            return aspect
        
        existing_description = ""
        existing_custom_properties = {}
        
        if aspect and isinstance(aspect, ContainerPropertiesClass): 
            existing_description = aspect.description or ""
            existing_custom_properties = aspect.customProperties or {}
        
        final_description = (
            new_description
            if new_description
            else existing_description
        )

        custom_properties = {**existing_custom_properties, **new_custom_properties}

        aspect.description = final_description
        aspect.customProperties = custom_properties
        return aspect
    
class AddTagTransformer(BaseTransformer, SingleAspectTransformer):
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
        Important: Currently does not obtain or remove existing tags. Existing tags would need to be,
        obtained via the ctx graph if required. 
        When the ingestion occurs tags will be added via the config, semantics control how they are added.

        Args:
            entity_urn (str): Urn of the container entity being ingested
            aspect_name (str): GlobalTagsClass
            aspect (Optional[Aspect]): GlobalTagsClass being transformed

        Returns:
            Optional[GlobalTagsClass]: List of GlobalTagsClass objects to be added to the container 
        """
        
        assert aspect is None or isinstance(aspect, GlobalTagsClass)

        if not self.config.tag_urns:
            logging.warning(
                f"No tag urns provided, skipping tag addition for {entity_urn}"
            )
            return aspect

        existing_tags = set()
        if aspect and isinstance(aspect, GlobalTagsClass):
            if aspect.tags:
                for tag_assoc in aspect.tags:
                    existing_tags.add(tag_assoc.tag)

        new_tags = []
        for tag_urn in self.config.tag_urns:
            if tag_urn not in existing_tags:
                new_tags.append(TagAssociationClass(tag=tag_urn))

        if not new_tags:
            return aspect

        updated_tags = []
        if aspect and isinstance(aspect, GlobalTagsClass) and aspect.tags:
            updated_tags.extend(aspect.tags)
        updated_tags.extend(new_tags)

        return GlobalTagsClass(tags=updated_tags)