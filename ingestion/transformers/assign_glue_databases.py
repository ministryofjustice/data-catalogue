import logging
from abc import ABCMeta
from typing import List, Optional, Union, Dict

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata._schema_classes import OwnerClass, BrowsePathsV2Class, DomainsClass
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from datahub.specific.dashboard import DashboardPatchBuilder

from ingestion.utils import report_time

URN_CONTAINER_PREFIX = "urn:li:container:"
DATAOWNER = "DATAOWNER"

logging.basicConfig(level=logging.DEBUG)


class AssignGlueDatabasesConfig(ConfigModel):
    data_custodian: str
    domain: str
    ownership_type: str


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

        logging.debug("Generating Ownership for containers")
        logging.debug(f"{self.config.domain=}, {self.config.data_custodian=}")

        containers = [urn for urn in self.entity_map.keys() if urn.startswith(URN_CONTAINER_PREFIX)]

        mcps: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = []

        for urn in containers:
            patch_builder = DashboardPatchBuilder(urn)

            if hasattr(self.config, 'ownership_type'):
                ownership_type = self.config.ownership_type
            else:
                ownership_type = DATAOWNER

            owner = OwnerClass(self.config.data_custodian, ownership_type)
            patch_builder.add_owner(owner)

            mcps.extend(list(patch_builder.build()))
            mcps.append(MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=DomainsClass(domains=[self.config.domain]),
                ))

        return mcps

