import json
from typing import Callable, Dict, List, Optional, Union, cast
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from datahub.configuration.common import (
    ConfigurationError,
    KeyValuePattern,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import DatasetDomainTransformer
from datahub.metadata.schema_classes import DomainsClass
from datahub.utilities.registries.domain_registry import DomainRegistry


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


class AddDatasetDomain(DatasetDomainTransformer):
    """Transformer that adds domains to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetDomainSemanticsConfig

    def __init__(self, config: AddDatasetDomainSemanticsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetDomain":
        config = AddDatasetDomainSemanticsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def raise_ctx_configuration_error(ctx: PipelineContext) -> None:
        if ctx.graph is None:
            raise ConfigurationError(
                "AddDatasetDomain requires a datahub_api to connect to. Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe"
            )

    @staticmethod
    def get_domain_class(
        graph: Optional[DataHubGraph], domains: List[str]
    ) -> DomainsClass:
        domain_registry: DomainRegistry = DomainRegistry(
            cached_domains=[k for k in domains], graph=graph
        )
        domain_class = DomainsClass(
            domains=[domain_registry.get_domain_urn(domain) for domain in domains]
        )
        return domain_class

    @staticmethod
    def _merge_with_server_domains(
        graph: DataHubGraph, urn: str, mce_domain: Optional[DomainsClass]
    ) -> Optional[DomainsClass]:
        if not mce_domain or not mce_domain.domains:
            # nothing to add, no need to consult server
            return None

        server_domain = graph.get_domain(entity_urn=urn)
        if server_domain:
            # compute patch
            # we only include domain who are not present in the server domain list
            domains_to_add: List[str] = []
            for domain in mce_domain.domains:
                if domain not in server_domain.domains:
                    domains_to_add.append(domain)
            # Lets patch
            mce_domain.domains = []
            mce_domain.domains.extend(server_domain.domains)
            mce_domain.domains.extend(domains_to_add)

        return mce_domain

    def transform_aspect(
        self, entity_urn: str, aspect_name, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_domain_aspect: DomainsClass = cast(DomainsClass, aspect)
        domain_aspect = DomainsClass(domains=[])
        # Check if we have received existing aspect
        if in_domain_aspect is not None and self.config.replace_existing is False:
            domain_aspect.domains.extend(in_domain_aspect.domains)

        domain_to_add = self.config.get_domains_to_add(entity_urn)

        domain_aspect.domains.extend(domain_to_add.domains)

        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            patch_domain_aspect: Optional[
                DomainsClass
            ] = AddDatasetDomain._merge_with_server_domains(
                self.ctx.graph, entity_urn, domain_aspect
            )
            return cast(Optional[Aspect], patch_domain_aspect)

        return cast(Optional[Aspect], domain_aspect)


class CadetAddDatasetDomain(AddDatasetDomain):
    """Transformer that adds a specified domains to each dataset."""

    def __init__(
        self, config: CadetDatasetDomainSemanticsConfig, ctx: PipelineContext
    ):
        AddDatasetDomain.raise_ctx_configuration_error(ctx)
        manifest = self._get_manifest(config)
        domain_pattern = self._get_domain_mapping(manifest)

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
    def create(
        cls, config_dict, ctx: PipelineContext
    ) -> "CadetAddDatasetDomain":
        try:
            manifest_s3_uri = config_dict.get("manifest_s3_uri")
        except Exception as e:
            print(e)
            raise
        config_dict = CadetDatasetDomainSemanticsConfig(
            manifest_s3_uri=manifest_s3_uri
        )
        return cls(config_dict, ctx)

    def _get_manifest(self, config_dict: CadetDatasetDomainSemanticsConfig) -> Dict:
        try:
            s3 = boto3.client("s3")
            s3_parts = config_dict.manifest_s3_uri.split("/")
            bucket_name = s3_parts[2]
            file_key = "/".join(s3_parts[3:])
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            manifest = json.loads(content)
        except NoCredentialsError:
            print("Credentials not available.")
            raise
        except ClientError as e:
            # If a client error is thrown, it will have a response attribute containing the error details
            error_code = e.response['Error']['Code']
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

    def _get_domain_mapping(self, manifest) -> Dict[str, DomainsClass]:
        """Map regex patterns for tables to domains for non seed nodes"""
        nodes = manifest.get("nodes")
        domain_mappings = {}

        for node in nodes:
            if manifest["nodes"][node]["resource_type"] == "seed":
                continue
            domain = manifest["nodes"][node].get("fqn")[1]
            node_table_name = "\.".join(manifest["nodes"][node].get("fqn")[-2:])
            node_table_pattern = f".*{node_table_name}.*"
            domain_mappings[node_table_pattern] = [domain]

        pattern_input = {"domain_pattern": {"rules": domain_mappings}}

        return PatternDatasetDomainSemanticsConfig.parse_obj(pattern_input).domain_pattern

# urn:li:dataset:(urn:li:dataPlatform:dbt,awsdatacatalog.common_platform_dev_v4.rd_observed_ethnicity,PROD)
