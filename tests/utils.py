from typing import Any, List, Optional, Type, Union, cast

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetTransformer,
    ContainerTransformer,
)
from datahub.metadata.schema_classes import MetadataChangeEventClass
from datahub.utilities.urns._urn_base import Urn


def make_generic_dataset_mcp(
    entity_urn: str = "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.prison_database.table1,PROD)",
    aspect_name: str = "status",
    aspect: Any = models.StatusClass(removed=False),
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=Urn.from_string(entity_urn).entity_type,
        aspectName=aspect_name,
        changeType="UPSERT",
        aspect=aspect,
    )


def make_generic_mcp(
    entity_urn: str = "urn:li:container:MOJDF_magistrates",
    aspect_name: str = "status",
    aspect: Any = models.StatusClass(removed=False),
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=Urn.from_string(entity_urn).entity_type,
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
                urn="urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.prison_database.table1,PROD)",
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


def run_container_transformer_pipeline(
    transformer_type: Type[ContainerTransformer],
    aspect: Optional[builder.Aspect],
    config: dict,
    pipeline_context: PipelineContext = PipelineContext(run_id="transformer_pipe_line"),
) -> List[RecordEnvelope]:
    transformer: ContainerTransformer = cast(
        ContainerTransformer, transformer_type.create(config, pipeline_context)
    )

    assert aspect
    container = make_generic_mcp(
        entity_urn="urn:li:container:abc",
        aspect=aspect,
        aspect_name=transformer.aspect_name(),
    )

    outputs = list(
        transformer.transform(
            [RecordEnvelope(input, metadata={}) for input in [container, EndOfStream()]]
        )
    )
    return outputs
