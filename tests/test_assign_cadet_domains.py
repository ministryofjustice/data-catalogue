from typing import cast

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DatahubClientConfig

from ingestion.transformers.assign_cadet_domains import AssignDerivedTableDomains
from utils import run_dataset_transformer_pipeline


class TestCadetTransformer:
    def test_pattern_add_dataset_domain_aspect_name(self, mock_datahub_graph):
        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        transformer = AssignDerivedTableDomains.create(
            {
                "manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
            },
            pipeline_context,
        )
        assert transformer.aspect_name() == models.DomainsClass.ASPECT_NAME

    def test_pattern_add_dataset_domain_match(self, mock_datahub_graph):
        prison_domain = builder.make_domain_urn("prison")

        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        output = run_dataset_transformer_pipeline(
            transformer_type=AssignDerivedTableDomains,
            aspect=models.DomainsClass(domains=[]),
            config={
                "manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(list, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 1
        assert prison_domain in transformed_aspect.domains

    def test_pattern_add_dataset_domain_overwrite(self, mock_datahub_graph):
        prison_domain = builder.make_domain_urn("prison")
        probation_domain = builder.make_domain_urn("probation")

        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        output = run_dataset_transformer_pipeline(
            transformer_type=AssignDerivedTableDomains,
            aspect=models.DomainsClass(domains=[probation_domain]),
            config={
                "manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json",
                "replace_existing": True,
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(list, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 1
        assert probation_domain not in transformed_aspect.domains
        assert prison_domain in transformed_aspect.domains
