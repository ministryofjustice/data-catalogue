from typing import cast

import datahub.emitter.mce_builder as builder
import datahub.emitter.mcp_builder as mcp_builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.metadata.schema_classes import TagAssociationClass
from utils import run_dataset_transformer_pipeline

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.transformers.assign_cadet_databases import AssignCadetDatabases


class TestAssignCadetDatabasesTransformer:
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

        output = run_dataset_transformer_pipeline(
            transformer_type=AssignCadetDatabases,
            aspect=models.GlobalTagsClass(tags=[]),
            config={
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 4
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.GlobalTagsClass)
        assert output[0].record.aspect.tags == [TagAssociationClass(tag=builder.make_tag_urn("Prison"))]
        assert isinstance(output[2].record.aspect, models.ContainerClass)
        assert output[2].record.aspect.container == expected_key.as_urn()
