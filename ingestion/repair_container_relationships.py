import argparse
import logging
import os
import re

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.metadata.schema_classes import ContainerClass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASET_URN_PATTERN = re.compile(
    r"^urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,]+),[^\)]+\)$"
)


def parse_dataset_database_and_table_from_urn(urn: str) -> tuple[str, str] | None:
    dataset_match = DATASET_URN_PATTERN.match(urn)
    if not dataset_match:
        return None

    dataset_name = dataset_match.group(2)
    if "." not in dataset_name:
        return None

    database_name, table_name = dataset_name.split(".", 1)
    return database_name, table_name


def find_target_dataset_urns(
    graph: DataHubGraph,
    dataset_database: str,
    table_prefix: str | None,
    platform: str | None,
    env: str | None,
    batch_size: int,
) -> list[str]:
    dataset_urns: list[str] = []
    seen: set[str] = set()

    logger.info(
        "Searching for dataset URNs to repair in database=%s table_prefix=%s platform=%s env=%s",
        dataset_database,
        table_prefix or "<none>",
        platform or "<none>",
        env or "<none>",
    )

    for urn in graph.get_urns_by_filter(
        entity_types=["dataset"],
        query=dataset_database,
        batch_size=batch_size,
        platform=platform,
        env=env,
        status=RemovedStatusFilter.NOT_SOFT_DELETED,
    ):
        if urn in seen:
            continue

        parsed = parse_dataset_database_and_table_from_urn(urn)
        if not parsed:
            continue

        database_name, table_name = parsed
        if database_name != dataset_database:
            continue

        if table_prefix and not table_name.startswith(table_prefix):
            continue

        seen.add(urn)
        dataset_urns.append(urn)

    return dataset_urns


def apply_container_relationship_repairs(
    graph: DataHubGraph,
    dataset_urns: list[str],
    container_urn: str,
) -> None:
    for dataset_urn in dataset_urns:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ContainerClass(container=container_urn),
            )
        )
        logger.info(
            "Applied IsPartOf repair dataset_urn=%s container_urn=%s",
            dataset_urn,
            container_urn,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Repair IsPartOf relationships by writing Container aspects for matching datasets. "
            "Defaults to dry-run unless --apply is provided."
        )
    )
    parser.add_argument("--dataset-database", required=True)
    parser.add_argument("--table-prefix", default=None)
    parser.add_argument("--container-urn", required=True)
    parser.add_argument("--platform", default=None)
    parser.add_argument("--env", default=None)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--show-sample", type=int, default=25)
    parser.add_argument("--apply", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
    datahub_gms_token = os.getenv("DATAHUB_GMS_TOKEN")

    if not datahub_gms_url or not datahub_gms_token:
        raise ValueError(
            "Missing required environment variables DATAHUB_GMS_URL and/or DATAHUB_GMS_TOKEN"
        )

    graph = DataHubGraph(
        DatahubClientConfig(
            server=datahub_gms_url,
            token=datahub_gms_token,
        )
    )

    dataset_urns = find_target_dataset_urns(
        graph=graph,
        dataset_database=args.dataset_database,
        table_prefix=args.table_prefix,
        platform=args.platform,
        env=args.env,
        batch_size=args.batch_size,
    )

    logger.info("Found %d datasets requiring relationship repair", len(dataset_urns))
    for dataset_urn in dataset_urns[: args.show_sample]:
        logger.info("repair sample dataset_urn=%s", dataset_urn)

    if not args.apply:
        logger.info("Dry run complete. Re-run with --apply to write Container relationships.")
        return 0

    apply_container_relationship_repairs(
        graph=graph,
        dataset_urns=dataset_urns,
        container_urn=args.container_urn,
    )
    logger.info("Completed IsPartOf repairs for %d datasets", len(dataset_urns))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
