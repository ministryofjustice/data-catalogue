from typing import Any, List, Optional, Type, Union, cast

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.dataset_transformer import (
    ContainerTransformer,
    DatasetTransformer,
)
from datahub.metadata.schema_classes import MetadataChangeEventClass, _Aspect
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


class EntityInspector:
    """
    Helper class for testing generated entity metadata
    """

    def __init__(self, urn):
        self.urn = urn
        self.aspects = {}

    def __bool__(self):
        # This object is truthy if it has aspects, otherwise falsey
        return bool(self.aspects)

    def add_aspect(self, aspect):
        self.aspects.setdefault(aspect.get_aspect_name(), []).append(aspect)

    def aspect(self, aspect):
        aspect = self.aspects.get(aspect, [])
        assert len(aspect) == 1
        return aspect[0]

    @property
    def tag_names(self):
        return [
            tag.tag
            for tagAspect in self.aspects.get("globalTags", [])
            for tag in tagAspect.tags
        ]


class WorkunitInspector:
    """
    Helper class for testing generated workunits
    """

    def __init__(self, workunits):
        self.workunits = workunits

        metadata_by_urn = {}
        for wu in workunits:
            urn = wu.get_urn()
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                aspect = wu.metadata.aspect
                entity_assertions = metadata_by_urn.setdefault(
                    urn, EntityInspector(urn)
                )
                entity_assertions.add_aspect(aspect)
            elif isinstance(wu.metadata, MetadataChangeEventClass):
                for aspect in wu.metadata.proposedSnapshot.aspects:
                    entity_assertions = metadata_by_urn.setdefault(
                        urn, EntityInspector(urn)
                    )
                    entity_assertions.add_aspect(aspect)

        self._metadata_by_urn = metadata_by_urn

    def entity(self, urn) -> EntityInspector:
        return self._metadata_by_urn.get(urn, EntityInspector(urn))

    @property
    def charts(self):
        return [
            entity
            for urn, entity in self._metadata_by_urn.items()
            if urn.startswith("urn:li:chart:")
        ]


def extract_tag_names(global_tags_list):
    return [tag.tag for association in global_tags_list for tag in association.tags]
