import logging
from abc import ABCMeta
from typing import List, Optional, Union

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import MetadataChangeProposalClass


from ingestion.utils import report_time

logging.basicConfig(level=logging.DEBUG)


class AssignGlueDatabasesConfig(ConfigModel):
    # dataset_urn -> data product urn
    manifest_s3_uri: str


class AssignGlueDatabases(DatasetTransformer, metaclass=ABCMeta):
    """Transformer that adds database container relationship
    for a provided dataset according to a manifest"""

    ctx: PipelineContext
    config: AssignGlueDatabasesConfig

    def __init__(self, config: AssignGlueDatabasesConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AssignGlueDatabases":
        config = AssignGlueDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def aspect_name(self):
        return "container"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        return None

    @report_time
    def handle_end_of_stream(
        self,
    ) -> List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:

        mcps: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = []

            return mcps

