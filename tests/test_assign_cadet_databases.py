from typing import cast

import datahub.emitter.mce_builder as builder
import datahub.emitter.mcp_builder as mcp_builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DatahubClientConfig
from utils import run_dataset_transformer_pipeline

from ingestion.transformers.assign_cadet_databases import AssignCadetDatabases
from ingestion.config import ENV, INSTANCE, PLATFORM


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
            aspect=models.StatusClass(removed=False),
            config={
                "manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 3
        assert output[1] is not None
        assert output[1].record is not None
        assert isinstance(output[1].record, MetadataChangeProposalWrapper)
        assert output[1].record.aspect is not None
        assert isinstance(output[1].record.aspect, models.ContainerClass)
        assert output[1].record.aspect.container == expected_key.as_urn()
