import logging
from abc import ABCMeta
from datetime import datetime
from typing import Dict, Optional, cast
from urllib.parse import urlparse

import boto3
import datahub.emitter.mce_builder as mce_builder
from botocore.exceptions import ClientError
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import DatasetPropertiesClass

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.ingestion_utils import (
    get_cadet_metadata_json,
    parse_database_and_table_names,
    validate_fqn,
)

logging.basicConfig(level=logging.INFO)


class AddLatestFileTimestampConfig(ConfigModel):
    manifest_s3_uri: str
    aws_region: str = "eu-west-1"


class AddLatestFileTimestamp(DatasetTransformer, metaclass=ABCMeta):
    """Adds latest_file_timestamp to dbt model dataset properties.

    For each dbt model in the manifest we resolve the underlying Glue table,
    then inspect objects in its S3 location to find the latest LastModified
    timestamp.
    """

    ctx: PipelineContext
    config: AddLatestFileTimestampConfig
    latest_file_timestamp_lookup: Dict[str, str]

    def __init__(self, config: AddLatestFileTimestampConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.glue_client = boto3.client("glue", region_name=self.config.aws_region)
        self.s3_client = boto3.client("s3", region_name=self.config.aws_region)
        manifest = get_cadet_metadata_json(self.config.manifest_s3_uri)
        self.latest_file_timestamp_lookup = self._build_latest_timestamp_lookup(manifest)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "AddLatestFileTimestamp":
        config = AddLatestFileTimestampConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def aspect_name(self) -> str:
        return "datasetProperties"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        if aspect is None:
            return None

        in_dataset_properties_aspect = cast(DatasetPropertiesClass, aspect)
        latest_file_timestamp = self.latest_file_timestamp_lookup.get(entity_urn)

        if latest_file_timestamp:
            custom_properties = in_dataset_properties_aspect.customProperties or {}
            custom_properties["latest_file_timestamp"] = latest_file_timestamp
            in_dataset_properties_aspect.customProperties = custom_properties

        return cast(Aspect, in_dataset_properties_aspect)

    def _build_latest_timestamp_lookup(self, manifest: dict) -> Dict[str, str]:
        lookup: Dict[str, str] = {}
        for node in manifest["nodes"].values():
            if node.get("resource_type") != "model":
                continue

            if not validate_fqn(node["fqn"]):
                continue

            database_name, table_name = parse_database_and_table_names(node)
            dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                name=f"{database_name}.{table_name}",
                platform=PLATFORM,
                platform_instance=INSTANCE,
                env=ENV,
            )

            try:
                table = self.glue_client.get_table(
                    DatabaseName=database_name,
                    Name=table_name,
                )
                location = (
                    table.get("Table", {})
                    .get("StorageDescriptor", {})
                    .get("Location", "")
                )
            except ClientError as error:
                logging.warning(
                    "Skipping latest_file_timestamp for %s.%s due to Glue error: %s",
                    database_name,
                    table_name,
                    error,
                )
                continue

            latest_last_modified = self._get_latest_last_modified(location)
            if latest_last_modified:
                lookup[dataset_urn] = latest_last_modified.isoformat()

        return lookup

    def _get_latest_last_modified(self, s3_uri: str) -> Optional[datetime]:
        parsed_s3_uri = urlparse(s3_uri)
        if parsed_s3_uri.scheme != "s3" or not parsed_s3_uri.netloc:
            return None

        bucket = parsed_s3_uri.netloc
        prefix = parsed_s3_uri.path.lstrip("/")

        latest_last_modified: Optional[datetime] = None
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for s3_object in page.get("Contents", []):
                object_last_modified = s3_object.get("LastModified")
                if (
                    object_last_modified
                    and (
                        latest_last_modified is None
                        or object_last_modified > latest_last_modified
                    )
                ):
                    latest_last_modified = object_last_modified

        return latest_last_modified