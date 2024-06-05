import json
import re
from typing import Callable, Dict, Union

import boto3
import datahub.emitter.mce_builder as builder
from botocore.exceptions import ClientError, NoCredentialsError
from datahub.configuration.common import (
    KeyValuePattern,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_domain import AddDatasetDomain
from datahub.metadata.schema_classes import DomainsClass


class AddDatasetDomainSemanticsConfig(TransformerSemanticsConfigModel):
    get_domains_to_add: Union[
        Callable[[str], DomainsClass],
        Callable[[str], DomainsClass],
    ]

    _resolve_domain_fn = pydantic_resolve_key("get_domains_to_add")


class PatternDatasetDomainSemanticsConfig(TransformerSemanticsConfigModel):
    domain_pattern: KeyValuePattern = KeyValuePattern.all()


class CadetDatasetDomainSemanticsConfig(TransformerSemanticsConfigModel):
    manifest_s3_uri: str


class AssignDerivedTableDomains(AddDatasetDomain):
    """Transformer that adds a specified domains to each dataset."""

    def __init__(self, config: CadetDatasetDomainSemanticsConfig, ctx: PipelineContext):
        AddDatasetDomain.raise_ctx_configuration_error(ctx)
        manifest = self._get_manifest(config)
        domain_mappings = self._get_domain_mapping(manifest)
        domain_pattern = domain_mappings.domain_pattern

        def resolve_domain(domain_urn: str) -> DomainsClass:
            domains = domain_pattern.value(domain_urn)
            return self.get_domain_class(ctx.graph, domains)

        generic_config = AddDatasetDomainSemanticsConfig(
            get_domains_to_add=resolve_domain,
            semantics=config.semantics,
            replace_existing=config.replace_existing,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict, ctx: PipelineContext) -> "AssignDerivedTableDomains":
        try:
            manifest_s3_uri = config_dict.get("manifest_s3_uri")
            replace_existing = config_dict.get("replace_existing", False)
        except Exception as e:
            print(e)
            raise

        config_dict = CadetDatasetDomainSemanticsConfig(
            manifest_s3_uri=manifest_s3_uri,
            replace_existing=replace_existing,
        )
        return cls(config_dict, ctx)

    def _get_manifest(self, config_dict: CadetDatasetDomainSemanticsConfig) -> Dict:
        try:
            s3 = boto3.client("s3")
            s3_parts = config_dict.manifest_s3_uri.split("/")
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

    def _get_domain_mapping(self, manifest) -> PatternDatasetDomainSemanticsConfig:
        """Map regex patterns for tables to domains"""
        nodes = manifest.get("nodes")
        domain_mappings = {}

        for node in nodes:
            if manifest["nodes"][node]["resource_type"] == "seed":
                continue
            domain = manifest["nodes"][node].get("fqn", [])[1]
            node_table_name = manifest["nodes"][node].get("fqn", [])[-1]
            node_table_name_no_double_underscore = node_table_name.replace("__", ".")
            urn = builder.make_dataset_urn_with_platform_instance(
                platform="dbt",
                platform_instance="awsdatacatalog",
                name=node_table_name_no_double_underscore,
            )
            escaped_urn_for_regex = re.escape(urn)
            domain_mappings[escaped_urn_for_regex] = [domain]

        pattern_input = {"domain_pattern": {"rules": domain_mappings}}

        return PatternDatasetDomainSemanticsConfig.parse_obj(pattern_input)
