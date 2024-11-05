from typing import cast

import datahub.emitter.mce_builder as builder
import datahub.emitter.mcp_builder as mcp_builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DatahubClientConfig
from utils import run_dataset_transformer_pipeline, run_container_transformer_pipeline

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.transformers.enrich_container_transformer import (
    EnrichContainerTransformer,
)


class TestEnrichContainerTransformer:
    def test_pattern_add_dataset_domain_match(self, mock_datahub_graph):

        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)
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
                "domain": "urn:li:domain:General",
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 4

        results = {}
        for o in output[:-1]:
            results[o.record.aspect.ASPECT_NAME] = o.record
        assert len(results) == 3

        for k in results:
            assert isinstance(results[k], MetadataChangeProposalWrapper)
            assert results[k].entityType == "container"
            assert results[k].changeType == "UPSERT"
            assert results[k].entityUrn == "urn:li:container:abc"
            assert results[k].aspectName == k

        assert isinstance(results["ownership"].aspect, models.OwnershipClass)
        assert (
            results["ownership"].aspect.owners[0].owner == "urn:li:corpuser:roy.keane"
        )
        assert results["ownership"].aspect.owners[0].type == "DATAOWNER"

        assert isinstance(results["domains"].aspect, models.DomainsClass)
        assert results["domains"].aspect.domains[0] == "urn:li:domain:General"

        assert isinstance(results["globalTags"].aspect, models.GlobalTagsClass)
        assert (
            results["globalTags"].aspect.tags[0].tag
            == "urn:li:tag:dc_display_in_catalogue"
        )