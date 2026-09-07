import argparse
import logging
import os
from collections import defaultdict

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.graph.filters import RemovedStatusFilter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Delete all entities discoverable in a DataHub environment slice. "
            "Defaults to dry-run for safety."
        )
    )
    parser.add_argument(
        "--env",
        required=True,
        help="DataHub env to wipe, e.g. DEV or PROD",
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Optional platform filter, e.g. glue, dbt, postgres",
    )
    parser.add_argument(
        "--entity-types",
        nargs="+",
        default=[
            "dataset",
            "container",
            "chart",
            "dashboard",
            "dataFlow",
            "dataJob",
            "assertion",
            "tag",
            "domain",
        ],
        help="Entity types to enumerate and delete",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Search page size",
    )
    parser.add_argument(
        "--max-entities",
        type=int,
        default=50000,
        help="Safety cap; requires --force if exceeded",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow deletion when candidate count exceeds max-entities",
    )
    parser.add_argument(
        "--hard-delete",
        action="store_true",
        help="Use hard delete (default is soft delete)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletion. If omitted, script runs in dry-run mode.",
    )
    parser.add_argument(
        "--show-sample",
        type=int,
        default=30,
        help="Number of sample URNs to print",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    gms_url = os.getenv("DATAHUB_GMS_URL")
    gms_token = os.getenv("DATAHUB_GMS_TOKEN")
    if not gms_url or not gms_token:
        raise ValueError(
            "Missing required environment variables DATAHUB_GMS_URL and/or DATAHUB_GMS_TOKEN"
        )

    graph = DataHubGraph(
        DatahubClientConfig(server=gms_url, token=gms_token)
    )

    candidates_by_type: dict[str, list[str]] = defaultdict(list)
    seen: set[str] = set()

    for entity_type in args.entity_types:
        logger.info(
            "Scanning entity type=%s env=%s platform=%s",
            entity_type,
            args.env,
            args.platform or "<none>",
        )
        for urn in graph.get_urns_by_filter(
            entity_types=[entity_type],
            query="",
            batch_size=args.batch_size,
            platform=args.platform,
            env=args.env,
            status=RemovedStatusFilter.NOT_SOFT_DELETED,
        ):
            if urn in seen:
                continue
            seen.add(urn)
            candidates_by_type[entity_type].append(urn)

    total = sum(len(v) for v in candidates_by_type.values())
    logger.info("Found %d candidate entities in env=%s", total, args.env)
    for entity_type in sorted(candidates_by_type):
        logger.info("  %s: %d", entity_type, len(candidates_by_type[entity_type]))

    sample = []
    for entity_type in sorted(candidates_by_type):
        for urn in candidates_by_type[entity_type]:
            sample.append((entity_type, urn))
            if len(sample) >= args.show_sample:
                break
        if len(sample) >= args.show_sample:
            break

    if sample:
        logger.info("Sample URNs:")
        for entity_type, urn in sample:
            logger.info("  [%s] %s", entity_type, urn)

    if total > args.max_entities and not args.force:
        raise ValueError(
            f"Candidate count {total} exceeds max-entities {args.max_entities}. Use --force to proceed."
        )

    if not args.apply:
        logger.info("Dry-run only. Re-run with --apply to delete entities.")
        return 0

    mode = "hard" if args.hard_delete else "soft"
    logger.info("Applying %s delete for %d entities", mode, total)

    success = 0
    failure = 0
    for entity_type in sorted(candidates_by_type):
        for urn in candidates_by_type[entity_type]:
            try:
                graph.delete_entity(urn, hard=args.hard_delete)
                success += 1
            except Exception:
                logger.exception("Failed deleting %s", urn)
                failure += 1

    logger.info("Delete finished: success=%d failure=%d", success, failure)
    return 0 if failure == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())