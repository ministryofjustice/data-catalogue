import json
from typing import Dict, Iterable
import logging

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.assign_cadet_databases_source.config import (
    AssignCadetDatabasesConfig,
)
from ingestion.create_derived_table_databases_source.source import \
    get_cadet_manifest


@config_class(AssignCadetDatabasesConfig)
class AssignCadetDatabases(Source):
    source_config: AssignCadetDatabasesConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: AssignCadetDatabasesConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = AssignCadetDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        manifest = get_cadet_manifest(self.source_config.manifest_s3_uri)
        mappings = self._get_table_database_mappings(manifest)

        for dataset_urn, database_key in mappings.items():
            logging.info(f"Assigning dataset {dataset_urn} to {database_key.database}")
            yield from mcp_builder.add_dataset_to_container(
                container_key=database_key,
                dataset_urn=dataset_urn,
            )

    def _get_table_database_mappings(self, manifest) -> Dict[str, mcp_builder.DatabaseKey]:
        mappings = {}
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] == "model":
                node_table_name = manifest["nodes"][node]["fqn"][-1]
                parts = node_table_name.split("__")
                database = parts[0]
                node_table_name_no_double_underscore = node_table_name.replace("__", ".")

                dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                    name=node_table_name_no_double_underscore,
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

                mappings[dataset_urn] = database_key

        return mappings

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
