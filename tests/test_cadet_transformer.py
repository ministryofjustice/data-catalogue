from typing import Any, List, Optional, Type, Union, cast

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import MetadataChangeEventClass
from datahub.utilities.urns.urn import Urn

from ingestion.transformers.assign_cadet_domains import AssignDerivedTableDomains


def make_generic_dataset_mcp(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:dbt,awsdatacatalog.prison_database.table1,PROD)",
    aspect_name: str = "status",
    aspect: Any = models.StatusClass(removed=False),
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=Urn.create_from_string(entity_urn).get_type(),
        aspectName=aspect_name,
        changeType="UPSERT",
        aspect=aspect,
    )


def run_dataset_transformer_pipeline(
    transformer_type: Type[DatasetTransformer],
    aspect: Optional[builder.Aspect],
    config: dict,
    pipeline_context: PipelineContext = PipelineContext(run_id="transformer_pipe_line"),
    use_mce: bool = False,
) -> List[RecordEnvelope]:
    transformer: DatasetTransformer = cast(
        DatasetTransformer, transformer_type.create(config, pipeline_context)
    )

    dataset: Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]
    if use_mce:
        dataset = MetadataChangeEventClass(
            proposedSnapshot=models.DatasetSnapshotClass(
                urn="urn:li:dataset:(urn:li:dataPlatform:dbt,awsdatacatalog.prison_database.table1,PROD)",
                aspects=[],
            )
        )
    else:
        assert aspect
        dataset = make_generic_dataset_mcp(
            aspect=aspect, aspect_name=transformer.aspect_name()
        )

    outputs = list(
        transformer.transform(
            [RecordEnvelope(input, metadata={}) for input in [dataset, EndOfStream()]]
        )
    )
    return outputs


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

    # A bug in datahub's transformers is that domains don't overwrite correctly.
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
