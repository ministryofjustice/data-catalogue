from typing import Any, List, Optional, Type, Union, cast
import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.graph.client import DatahubClientConfig
from ingestion.cadet_transformer import (
    CadetAddDatasetDomain,
)
from datahub.configuration.common import TransformerSemantics
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass
)
from moto import mock_s3
from datahub.utilities.urns.urn import Urn


def make_generic_dataset_mcp(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
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
                urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)",
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

        transformer = CadetAddDatasetDomain.create(
            {"manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"}, pipeline_context
        )
        assert transformer.aspect_name() == models.DomainsClass.ASPECT_NAME

    def test_pattern_add_dataset_domain_match(self, mock_datahub_graph):
        acryl_domain = builder.make_domain_urn("acryl.io")
        gslab_domain = builder.make_domain_urn("gslab.io")

        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        output = run_dataset_transformer_pipeline(
            transformer_type=CadetAddDatasetDomain,
            aspect=models.DomainsClass(domains=[gslab_domain]),
            config={"manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"},
            pipeline_context=pipeline_context,
        )
        print(output)
        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 2
        assert gslab_domain in transformed_aspect.domains
        assert acryl_domain in transformed_aspect.domains

    def test_pattern_add_dataset_domain_no_match(self, mock_datahub_graph):
        acryl_domain = builder.make_domain_urn("acryl.io")
        gslab_domain = builder.make_domain_urn("gslab.io")
        pattern = "urn:li:dataset:\\(urn:li:dataPlatform:invalid,.*"

        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        output = run_dataset_transformer_pipeline(
            transformer_type=CadetAddDatasetDomain,
            aspect=models.DomainsClass(domains=[gslab_domain]),
            config={"manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"},
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 1
        assert gslab_domain in transformed_aspect.domains
        assert acryl_domain not in transformed_aspect.domains

    def test_pattern_add_dataset_domain_replace_existing_match(self, mock_datahub_graph):
        acryl_domain = builder.make_domain_urn("acryl.io")
        gslab_domain = builder.make_domain_urn("gslab.io")
        pattern = "urn:li:dataset:\\(urn:li:dataPlatform:bigquery,.*"

        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        output = run_dataset_transformer_pipeline(
            transformer_type=CadetAddDatasetDomain,
            aspect=models.DomainsClass(domains=[gslab_domain]),
            config={
                "replace_existing": True,
                "domain_pattern": {"rules": {pattern: [acryl_domain]}},
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 1
        assert gslab_domain not in transformed_aspect.domains
        assert acryl_domain in transformed_aspect.domains

    def test_pattern_add_dataset_domain_replace_existing_no_match(self, mock_datahub_graph):
        acryl_domain = builder.make_domain_urn("acryl.io")
        gslab_domain = builder.make_domain_urn("gslab.io")
        pattern = "urn:li:dataset:\\(urn:li:dataPlatform:invalid,.*"

        pipeline_context: PipelineContext = PipelineContext(
            run_id="test_simple_add_dataset_domain"
        )
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig)

        output = run_dataset_transformer_pipeline(
            transformer_type=CadetAddDatasetDomain,
            aspect=models.DomainsClass(domains=[gslab_domain]),
            config={
                "replace_existing": True,
                "domain_pattern": {"rules": {pattern: [acryl_domain]}},
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 0

    def test_pattern_add_dataset_domain_semantics_overwrite(self, mock_datahub_graph):
        acryl_domain = builder.make_domain_urn("acryl.io")
        gslab_domain = builder.make_domain_urn("gslab.io")
        server_domain = builder.make_domain_urn("test.io")
        pattern = "urn:li:dataset:\\(urn:li:dataPlatform:bigquery,.*"

        pipeline_context = PipelineContext(run_id="transformer_pipe_line")
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

        # Return fake aspect to simulate server behaviour
        def fake_get_domain(entity_urn: str) -> models.DomainsClass:
            return models.DomainsClass(domains=[server_domain])

        pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

        output = run_dataset_transformer_pipeline(
            transformer_type=CadetAddDatasetDomain,
            aspect=models.DomainsClass(domains=[gslab_domain]),
            config={
                "semantics": TransformerSemantics.OVERWRITE,
                "domain_pattern": {"rules": {pattern: [acryl_domain]}},
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 2
        assert gslab_domain in transformed_aspect.domains
        assert acryl_domain in transformed_aspect.domains
        assert server_domain not in transformed_aspect.domains

    def test_pattern_add_dataset_domain_semantics_patch(
        self, pytestconfig, tmp_path, mock_time, mock_datahub_graph
    ):
        acryl_domain = builder.make_domain_urn("acryl.io")
        gslab_domain = builder.make_domain_urn("gslab.io")
        server_domain = builder.make_domain_urn("test.io")
        pattern = "urn:li:dataset:\\(urn:li:dataPlatform:bigquery,.*"

        pipeline_context = PipelineContext(run_id="transformer_pipe_line")
        pipeline_context.graph = mock_datahub_graph(DatahubClientConfig())

        # Return fake aspect to simulate server behaviour
        def fake_get_domain(entity_urn: str) -> models.DomainsClass:
            return models.DomainsClass(domains=[server_domain])

        pipeline_context.graph.get_domain = fake_get_domain  # type: ignore

        output = run_dataset_transformer_pipeline(
            transformer_type=CadetAddDatasetDomain,
            aspect=models.DomainsClass(domains=[gslab_domain]),
            config={
                "replace_existing": False,
                "semantics": TransformerSemantics.PATCH,
                "domain_pattern": {"rules": {pattern: [acryl_domain]}},
            },
            pipeline_context=pipeline_context,
        )

        assert len(output) == 2
        assert output[0] is not None
        assert output[0].record is not None
        assert isinstance(output[0].record, MetadataChangeProposalWrapper)
        assert output[0].record.aspect is not None
        assert isinstance(output[0].record.aspect, models.DomainsClass)
        transformed_aspect = cast(models.DomainsClass, output[0].record.aspect)
        assert len(transformed_aspect.domains) == 3
        assert gslab_domain in transformed_aspect.domains
        assert acryl_domain in transformed_aspect.domains
        assert server_domain in transformed_aspect.domains
