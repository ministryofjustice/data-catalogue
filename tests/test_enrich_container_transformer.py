from unittest.mock import MagicMock

import datahub.emitter.mcp_builder as mcp_builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.metadata._schema_classes import ContainerPropertiesClass
from utils import run_container_transformer_pipeline

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.transformers.enrich_container_transformer import (
    EnrichContainerTransformer,
)


class TestEnrichContainerTransformer:
    def test_pattern_add_dataset_domain_match(self, mock_datahub_graph):

        pipeline_context: PipelineContext = PipelineContext(run_id="abc")
        graph = mock_datahub_graph(DatahubClientConfig)
        graph.get_aspect = MagicMock(
            return_value=ContainerPropertiesClass(name="foo", customProperties=None)
        )
        pipeline_context.graph = graph
        expected_key = mcp_builder.DatabaseKey(
            database="prison_database",
            platform=PLATFORM,
            instance=INSTANCE,
            env=ENV,
            backcompat_env_as_instance=True,
        )

        output = run_container_transformer_pipeline(
            transformer_type=EnrichContainerTransformer,
            aspect=models.ContainerClass(container=expected_key.database),
            config={
                "data_custodian": "urn:li:corpuser:roy.keane",
                "subject_areas": ["Prison"],
                "properties": {"security_classification": "Official-Sensitive"},
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 4

        results = {}
        for o in output[:-1]:
            results[o.record.aspect.ASPECT_NAME] = o.record

        for v in results.values():
            assert isinstance(v, MetadataChangeProposalWrapper)
            assert v.entityType == "container"
            assert v.changeType == "UPSERT"
            assert v.entityUrn == "urn:li:container:abc"

        assert isinstance(results["ownership"].aspect, models.OwnershipClass)
        assert (
            results["ownership"].aspect.owners[0].owner == "urn:li:corpuser:roy.keane"
        )
        assert results["ownership"].aspect.owners[0].type == "DATAOWNER"

        assert isinstance(results["globalTags"].aspect, models.GlobalTagsClass)
        assert (
            results["globalTags"].aspect.tags[0].tag
            == "urn:li:tag:dc_display_in_catalogue"
        )

        assert results["containerProperties"]

        assert results["containerProperties"].aspect.customProperties == {
            "security_classification": "Official-Sensitive"
        }
