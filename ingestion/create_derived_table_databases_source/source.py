import json
from typing import Dict, Iterable
import logging

import boto3
import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from botocore.exceptions import ClientError, NoCredentialsError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import ChangeTypeClass, DomainPropertiesClass
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.create_derived_table_databases_source.config import (
    CreateDerivedTableDatabasesConfig,
)


@config_class(CreateDerivedTableDatabasesConfig)
class CreateDerivedTableDatabases(Source):
    source_config: CreateDerivedTableDatabasesConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: CreateDerivedTableDatabasesConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = CreateDerivedTableDatabasesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        manifest = get_cadet_manifest(self.source_config.manifest_s3_uri)

        # Create all the domain entities
        for domain_name in self._get_domains(manifest):
            mcp = self._make_domain(domain_name)
            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            self.report.report_workunit(wu)
            logging.info(f"Creating domain {domain_name}")

            yield wu

        # Create database entities and assign them to their domains
        databases_with_domains = self._get_databases_with_domains(manifest)
        sub_types = [DatasetContainerSubTypes.DATABASE]
        for database, domain in databases_with_domains:
            database_container_key = mcp_builder.DatabaseKey(
                database=database,
                platform=PLATFORM,
                instance=INSTANCE,
                env=ENV,
                backcompat_env_as_instance=True,
            )
            domain_urn = mce_builder.make_domain_urn(domain=domain)
            logging.info(f"Creating container for database {database}")
            yield from mcp_builder.gen_containers(
                container_key=database_container_key,
                name=database,
                sub_types=sub_types,
                domain_urn=domain_urn,
                external_url=None,
                description=None,
                created=None,
                last_modified=None,
                tags=None,
                owner_urn=None,
                qualified_name=None,
                extra_properties=None,
            )

    def _get_domains(self, manifest) -> set[str]:
        """Only models are arranged by domain in CaDeT"""
        return set(
            manifest["nodes"][node]["fqn"][1]
            for node in manifest["nodes"]
            if manifest["nodes"][node]["resource_type"] == "model"
        )

    def _get_databases_with_domains(self, manifest) -> list[tuple[str, str]]:
        """
        These mappings will only work with tables named {database}__{table}
        like create a derived table
        """
        mappings = set()
        for node in manifest["nodes"]:
            if manifest["nodes"][node]["resource_type"] == "model":
                database = manifest["nodes"][node]["fqn"][-1].split("__")[0]
                domain = manifest["nodes"][node]["fqn"][1]
                mappings.add((database, domain))
        return mappings

    def _make_domain(self, domain_name) -> MetadataChangeProposalWrapper:
        domain_urn = mce_builder.make_domain_urn(domain=domain_name)
        domain_properties = DomainPropertiesClass(name=domain_name)
        metadata_event = MetadataChangeProposalWrapper(
            entityType="domain",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=domain_urn,
            aspect=domain_properties,
        )
        return metadata_event

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass


def get_cadet_manifest(manifest_s3_uri: str) -> Dict:
    try:
        s3 = boto3.client("s3")
        s3_parts = manifest_s3_uri.split("/")
        bucket_name = s3_parts[2]
        file_key = "/".join(s3_parts[3:])
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        manifest = json.loads(content, strict=False)
    except NoCredentialsError:
        print("Credentials not available.")
        raise
    except ClientError as e:
        # If a client error is thrown, it will have a response attribute containing the error details
        error_code = e.response["Error"]["Code"]
        print(f"Client error occurred: {error_code}")
        raise
    except json.JSONDecodeError:
        print("Error decoding manifest JSON.")
        raise
    except Exception as e:
        # Catch any other exceptions
        print(f"An error occurred: {str(e)}")
        raise
    return manifest
