from typing import cast
from unittest.mock import MagicMock

import datahub.emitter.mcp_builder as mcp_builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.metadata.schema_classes import ContainerPropertiesClass
from utils import run_container_transformer_pipeline

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.transformers.enrich_container_transformer import (
    EnrichContainerTransformer,
)

from ingestion.transformers.enrich_container import (
    AddOwnershipTransformer, 
    AddOwnershipTransformerConfig, 
    AddTagTransformer,
    AddTagTransformerConfig,
    AddPropertiesTransformer,
    AddPropertiesTransformerConfig
)

class TestAddOwnershipTransformer:
    def test_simple_add_ownership_transformer_overwrite(self):
        transformer = AddOwnershipTransformer(
            config=AddOwnershipTransformerConfig(
                data_custodian="urn:li:corpuser:test.user"
            ),
            ctx=PipelineContext(run_id="test_run"),
        )

        aspect = transformer.transform_aspect(
            entity_urn="urn:li:container:test_container",
            aspect_name="ownership",
            aspect=None,
        )

        assert isinstance(aspect, models.OwnershipClass)
        assert len(aspect.owners) == 1
        assert aspect.owners[0].owner == "urn:li:corpuser:test.user"
        assert aspect.owners[0].type == "DATAOWNER"

class TestAddPropertiesTransformer:
    def test_add_properties_transformer_overwrite_description_and_properties(self):
        pipeline_context: PipelineContext = PipelineContext(run_id="test_run")
        transformer = AddPropertiesTransformer(
            config=AddPropertiesTransformerConfig(
                description="Test container description",
                properties={"key1": "value1", "key2": "value2"},
            ),
            ctx=pipeline_context,
        )

        container_properties: ContainerPropertiesClass = ContainerPropertiesClass(
            name="test_container",
            description="old description",
            customProperties={"old_key": "old_value"},
        )

        aspect = transformer.transform_aspect(
            entity_urn="urn:li:container:test_container",
            aspect_name="containerProperties",
            aspect=container_properties,
        )

        assert aspect
        assert isinstance(aspect, ContainerPropertiesClass)
        assert aspect.name == "test_container"
        assert aspect.description == "Test container description"
        assert aspect.customProperties == {"key1": "value1", "key2": "value2"}
    
    def test_add_properties_transformer_overwrite_properties(self):
        pipeline_context: PipelineContext = PipelineContext(run_id="test_run")
        transformer = AddPropertiesTransformer(
            config=AddPropertiesTransformerConfig(
                properties={"key1": "value1", "key2": "value2"},
            ),
            ctx=pipeline_context,
        )

        container_properties: ContainerPropertiesClass = ContainerPropertiesClass(
            name="test_container",
            description="old description",
            customProperties={"old_key": "old_value"},
        )

        aspect = transformer.transform_aspect(
            entity_urn="urn:li:container:test_container",
            aspect_name="containerProperties",
            aspect=container_properties,
        )

        assert aspect
        assert isinstance(aspect, ContainerPropertiesClass)
        assert aspect.name == "test_container"
        assert aspect.description == "old description"
        assert aspect.customProperties == {"key1": "value1", "key2": "value2"}

    def test_add_properties_transformer_overwrite_description(self):
        pipeline_context: PipelineContext = PipelineContext(run_id="test_run")
        transformer = AddPropertiesTransformer(
            config=AddPropertiesTransformerConfig(
                description="Test container description",
            ),
            ctx=pipeline_context,
        )

        container_properties: ContainerPropertiesClass = ContainerPropertiesClass(
            name="test_container",
            description="old description",
            customProperties={"old_key": "old_value"},
        )

        aspect = transformer.transform_aspect(
            entity_urn="urn:li:container:test_container",
            aspect_name="containerProperties",
            aspect=container_properties,
        )

        assert aspect
        assert isinstance(aspect, ContainerPropertiesClass)
        assert aspect.name == "test_container"
        assert aspect.description == "Test container description"
        assert aspect.customProperties == {"old_key": "old_value"}

class TestAddTagTransformer:
    def test_add_tag_transformer_overwrite(self):
        pipeline_context: PipelineContext = PipelineContext(run_id="test_run")
        transformer = AddTagTransformer(
            config=AddTagTransformerConfig(
                tag_urns=[
                    "urn:li:tag:tag1",
                    "urn:li:tag:tag2",
                ],
            ),
            ctx=pipeline_context,
        )

        global_tags: models.GlobalTagsClass = models.GlobalTagsClass(
            tags=[
                models.TagAssociationClass(tag="urn:li:tag:old_tag"),
            ]
        )

        aspect = transformer.transform_aspect(
            entity_urn="urn:li:container:test_container",
            aspect_name="globalTags",
            aspect=global_tags,
        )

        assert aspect
        assert isinstance(aspect, models.GlobalTagsClass)
        assert len(aspect.tags) == 2
        assert aspect.tags[0].tag == "urn:li:tag:tag1"
        assert aspect.tags[1].tag == "urn:li:tag:tag2"