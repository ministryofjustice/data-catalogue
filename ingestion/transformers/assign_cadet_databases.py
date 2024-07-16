import logging
from abc import ABCMeta
from typing import Dict, List, Optional, Union

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import ContainerClass, MetadataChangeProposalClass

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.dbt_manifest_utils import (
    get_cadet_manifest,
    validate_fqn,
    parse_database_and_table_names,
)


class AssignCadetDatabasesConfig(ConfigModel):
    # dataset_urn -> data product urn
    manifest_s3_uri: str


class AssignCadetDatabases(DatasetTransformer, metaclass=ABCMeta):
    """Transformer that adds database container relationship
    for a provided dataset according to a manifest"""

    ctx: PipelineContext
    config: AssignCadetDatabasesConfig

    def __init__(self, config: AssignCadetDatabasesConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AssignCadetDatabases":
        config = AssignCadetDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def aspect_name(self):
        return "container"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        return None

    def handle_end_of_stream(
        self,
    ) -> List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:

        mcps: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = []

        manifest = get_cadet_manifest(self.config.manifest_s3_uri)
        mappings = self._get_table_database_mappings(manifest)

        logging.debug("Assigning databases to datasets")
        for dataset_urn in self.entity_map.keys():
            container_urn = mappings.get(dataset_urn)
            if not container_urn:
                logging.warning(f"No container mapping for {dataset_urn=}")
                continue

            logging.info(f"Assigning {dataset_urn=} to {container_urn=}")
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=f"{dataset_urn}",
                    aspect=ContainerClass(container=f"{container_urn}"),
                )
            )

        return mcps

    def _get_table_database_mappings(self, manifest) -> Dict[str, str]:
        mappings = {}
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] == "model":
                fqn = manifest["nodes"][node]["fqn"]
                if validate_fqn(fqn):
                    database, table_name = parse_database_and_table_names(
                        manifest["nodes"][node]
                    )

                    dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                        name=f"{database}.{table_name}",
                        platform=PLATFORM,
                        platform_instance=INSTANCE,
                        env=ENV,
                    )
                    database_key = mcp_builder.DatabaseKey(
                        database=database,
                        platform=PLATFORM,
                        instance=INSTANCE,
                        env=ENV,
                        backcompat_env_as_instance=True,
                    )
                    database_urn = database_key.as_urn()

                    mappings[dataset_urn] = database_urn

        return mappings
