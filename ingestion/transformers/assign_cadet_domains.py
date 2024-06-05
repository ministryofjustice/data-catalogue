import re
from typing import Callable, Tuple, Union

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (KeyValuePattern,
                                          TransformerSemanticsConfigModel)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_domain import AddDatasetDomain
from datahub.metadata.schema_classes import DomainsClass

from ingestion.create_derived_table_domains_source.source import \
    get_cadet_manifest


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
        manifest = get_cadet_manifest(config.manifest_s3_uri)
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

    def _get_domain_mapping(self, manifest) -> PatternDatasetDomainSemanticsConfig:
        """Map regex patterns for tables to domains"""
        nodes = manifest.get("nodes")
        domain_mappings = {}

        for node in nodes:
            node_info = nodes[node]
            if node_info["resource_type"] != "model":
                continue
            domain, escaped_urn_for_regex = self._convert_cadet_manifest_table_to_datahub(node_info)
            domain_mappings[escaped_urn_for_regex] = [domain]

        pattern_input = {"domain_pattern": {"rules": domain_mappings}}

        return PatternDatasetDomainSemanticsConfig.parse_obj(pattern_input)

    def _convert_cadet_manifest_table_to_datahub(self, node_info: dict) -> Tuple[str, str]:
        domain = node_info.get("fqn", [])[1]
        node_table_name = node_info.get("fqn", [])[-1]

        # In CaDeT the convention is to name a table database__table
        node_table_name_no_double_underscore = node_table_name.replace("__", ".")
        urn = builder.make_dataset_urn_with_platform_instance(
                platform="dbt",
                platform_instance="cadet.awsdatacatalog",
                name=node_table_name_no_double_underscore,
            )
        escaped_urn_for_regex = re.escape(urn)

        return domain, escaped_urn_for_regex
