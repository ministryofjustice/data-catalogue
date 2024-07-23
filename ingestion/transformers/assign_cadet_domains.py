import logging
import time
from typing import Callable, Union, Iterable, List
from datahub.ingestion.api.common import RecordEnvelope

from datahub.configuration.common import (
    KeyValuePattern,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_domain import AddDatasetDomain
from datahub.metadata.schema_classes import DomainsClass

from ingestion.dbt_manifest_utils import (
    convert_cadet_manifest_table_to_datahub,
    get_cadet_manifest,
    validate_fqn,
)


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


class AssignCadetDomains(AddDatasetDomain):
    """Transformer that adds a specified domains to each dataset."""

    def __init__(self, config: CadetDatasetDomainSemanticsConfig, ctx: PipelineContext):
        logging.info("Assign Cadet Domains INIT")
        self.time_spent = 0
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

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        start = time.perf_counter_ns()
        val = super().transform(record_envelopes=record_envelopes)
        end = time.perf_counter_ns()
        # diff = (end - start) / 1e9
        self.time_spent += end - start
        # logging.info(
        #     f"*** assign_cadet_domains transform - time taken: {diff:.2f} seconds"
        # )
        return val

    def handle_end_of_stream(
        self,
    ):
        diff = self.time_spent / 1e9

        logging.info(
            f"*** assign_cadet_domains handle_end_of_streams - time taken: {diff:.2f} seconds"
        )

        return super().handle_end_of_stream()

    @classmethod
    def create(cls, config_dict, ctx: PipelineContext) -> "AssignCadetDomains":
        logging.info("*** creating instance of AssignCadetDomains ***")
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
        start = time.time()
        nodes = manifest.get("nodes")
        domain_mappings = {}

        for node in nodes:
            node_info = nodes[node]
            if node_info["resource_type"] != "model":
                continue
            if validate_fqn(nodes[node]["fqn"]):
                domain, escaped_urn_for_regex = convert_cadet_manifest_table_to_datahub(
                    node_info
                )
                domain_mappings[escaped_urn_for_regex] = [domain]

        pattern_input = {"domain_pattern": {"rules": domain_mappings}}
        end = time.time()
        diff = end - start
        logging.info(f"*** get domain mapping - time taken: {diff} seconds")
        return PatternDatasetDomainSemanticsConfig.parse_obj(pattern_input)
