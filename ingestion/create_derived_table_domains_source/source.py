import json
from typing import Dict, Iterable

import boto3
import datahub.emitter.mce_builder as builder
from botocore.exceptions import ClientError, NoCredentialsError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import ChangeTypeClass, DomainPropertiesClass

from ingestion.create_derived_table_domains_source.config import (
    CreateDerivedTableDomainsConfig,
)


@config_class(CreateDerivedTableDomainsConfig)
class CreateDerivedTableDomains(Source):
    source_config: CreateDerivedTableDomainsConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: CreateDerivedTableDomainsConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = CreateDerivedTableDomainsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        manifest = self._get_manifest(self.source_config.manifest_s3_uri)
        for domain_name in self._get_domains(manifest):
            mcp = self._make_domain(domain_name)
            wu = MetadataWorkUnit("single_mcp", mcp=mcp)
            self.report.report_workunit(wu)

            yield wu

    def _get_domains(self, manifest) -> set[str]:
        return set(
            manifest["nodes"][node]["fqn"][1]
            for node in manifest["nodes"]
            if not manifest["nodes"][node]["resource_type"] == "seed"
        )

    def _get_manifest(self, manifest_s3_uri: str) -> Dict:
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

    def _make_domain(self, domain_name) -> MetadataChangeProposalWrapper:
        domain_urn = builder.make_domain_urn(domain=domain_name)
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
