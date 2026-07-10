import argparse
import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.graph.filters import RemovedStatusFilter

EXCLUDED_NAME_PATTERNS = (
    "stg",
    "staging",
    "int",
    "dummy",
    "intermediate",
    "dev",
    "test",
    "testing",
    "temp",
    "example",
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATASET_URN_PATTERN = re.compile(
    r"^urn:li:dataset:\\(urn:li:dataPlatform:([^,]+),([^,]+),[^\\)]+\\)$"
)
CONTAINER_URN_PATTERN = re.compile(r"^urn:li:container:(.+)$")
PROTECTED_DATA_PLATFORMS = {"gov.uk"}


@dataclass(frozen=True)
class CandidateEntity:
    urn: str
    matched_pattern: str


def matches_pattern_with_boundaries(value: str, pattern: str) -> bool:
    return (
        re.search(rf"(^|[^a-z0-9]){re.escape(pattern)}([^a-z0-9]|$)", value.lower())
        is not None
    )


def _extract_name_from_container_entity_raw(entity_raw: dict) -> str | None:
    aspects = entity_raw.get("aspects", {})
    container_props = aspects.get("containerProperties", {})

    if not isinstance(container_props, dict):
        return None

    # Depending on API endpoint/version this can either be direct payload
    # or wrapped under a `value` key.
    if isinstance(container_props.get("name"), str):
        return container_props.get("name")

    value = container_props.get("value")
    if isinstance(value, dict) and isinstance(value.get("name"), str):
        return value.get("name")

    return None


def get_container_display_name(graph: DataHubGraph, urn: str) -> str | None:
    try:
        entity_raw = graph.get_entity_raw(urn, aspects=["containerProperties"])
    except Exception:
        logger.exception("Failed to fetch containerProperties for urn=%s", urn)
        return None

    return _extract_name_from_container_entity_raw(entity_raw)


def parse_name_from_urn(urn: str) -> str:
    dataset_match = DATASET_URN_PATTERN.match(urn)
    if dataset_match:
        return dataset_match.group(2)

    container_match = CONTAINER_URN_PATTERN.match(urn)
    if container_match:
        return container_match.group(1)

    return urn


def parse_dataset_platform_from_urn(urn: str) -> str | None:
    dataset_match = DATASET_URN_PATTERN.match(urn)
    if not dataset_match:
        return None

    return dataset_match.group(1)


def parse_dataset_database_and_table_from_urn(urn: str) -> tuple[str, str] | None:
    dataset_match = DATASET_URN_PATTERN.match(urn)
    if not dataset_match:
        return None

    dataset_name = dataset_match.group(2)
    if "." not in dataset_name:
        return None

    database_name, table_name = dataset_name.split(".", 1)
    return database_name, table_name


def is_protected_urn(urn: str) -> bool:
    if not urn.startswith("urn:li:dataset:"):
        return False

    platform = parse_dataset_platform_from_urn(urn)
    if not platform:
        return False

    return platform.lower() in PROTECTED_DATA_PLATFORMS


def is_dataset_in_scope(
    urn: str,
    dataset_database: str | None,
    keep_table_prefix: str | None,
) -> bool:
    if not urn.startswith("urn:li:dataset:"):
        return True

    if not dataset_database and not keep_table_prefix:
        return True

    dataset_parts = parse_dataset_database_and_table_from_urn(urn)
    if not dataset_parts:
        return False

    database_name, table_name = dataset_parts

    if dataset_database and database_name != dataset_database:
        return False

    if keep_table_prefix and table_name.startswith(keep_table_prefix):
        return False

    return True


def find_candidates(
    graph: DataHubGraph,
    patterns: Iterable[str],
    entity_types: list[str],
    batch_size: int,
    platform: str | None,
    env: str | None,
    require_display_tag: bool,
    dataset_database: str | None,
    keep_table_prefix: str | None,
) -> list[CandidateEntity]:
    seen_urns: set[str] = set()
    candidates: list[CandidateEntity] = []
    extra_filters = None
    container_name_cache: dict[str, str | None] = {}

    if require_display_tag:
        extra_filters = [
            {
                "field": "tags",
                "condition": "EQUAL",
                "values": ["urn:li:tag:dc_display_in_catalogue"],
            }
        ]

    for pattern in patterns:
        query = pattern
        logger.info("Searching for entities with query=%s", query)

        for urn in graph.get_urns_by_filter(
            entity_types=entity_types,
            query=query,
            batch_size=batch_size,
            platform=platform,
            env=env,
            status=RemovedStatusFilter.NOT_SOFT_DELETED,
            extraFilters=extra_filters,
        ):
            if urn in seen_urns:
                continue

            if is_protected_urn(urn):
                logger.info("Skipping protected platform entity urn=%s", urn)
                continue

            if not is_dataset_in_scope(urn, dataset_database, keep_table_prefix):
                logger.info("Skipping out-of-scope dataset urn=%s", urn)
                continue

            # DataHub full-text search can return broad matches for short
            # patterns (for example, "int"). For dataset entities, enforce a
            # strict substring check against the parsed dataset name.
            if urn.startswith("urn:li:dataset:"):
                parsed_name = parse_name_from_urn(urn).lower()
                if not matches_pattern_with_boundaries(parsed_name, pattern):
                    continue

            if urn.startswith("urn:li:container:"):
                if urn not in container_name_cache:
                    container_name_cache[urn] = get_container_display_name(graph, urn)

                display_name = container_name_cache[urn]
                if not display_name:
                    # Without display name, skip to avoid false positives.
                    continue

                if not matches_pattern_with_boundaries(display_name, pattern):
                    continue

            # Trust DataHub's full-text search match for the query pattern.
            # This avoids false negatives for entities like containers where
            # the URN does not contain the display name.
            candidates.append(CandidateEntity(urn=urn, matched_pattern=pattern))
            seen_urns.add(urn)

    return candidates


def find_database_scope_candidates(
    graph: DataHubGraph,
    batch_size: int,
    platform: str | None,
    env: str | None,
    require_display_tag: bool,
    dataset_database: str | None,
    keep_table_prefix: str | None,
) -> list[CandidateEntity]:
    if not dataset_database:
        return []

    extra_filters = None
    if require_display_tag:
        extra_filters = [
            {
                "field": "tags",
                "condition": "EQUAL",
                "values": ["urn:li:tag:dc_display_in_catalogue"],
            }
        ]

    candidates: list[CandidateEntity] = []
    seen_urns: set[str] = set()

    logger.info(
        "Searching for database-scoped datasets in database=%s", dataset_database
    )

    for urn in graph.get_urns_by_filter(
        entity_types=["dataset"],
        query=dataset_database,
        batch_size=batch_size,
        platform=platform,
        env=env,
        status=RemovedStatusFilter.NOT_SOFT_DELETED,
        extraFilters=extra_filters,
    ):
        if urn in seen_urns:
            continue

        if is_protected_urn(urn):
            logger.info("Skipping protected platform entity urn=%s", urn)
            continue

        if not is_dataset_in_scope(urn, dataset_database, keep_table_prefix):
            continue

        candidates.append(CandidateEntity(urn=urn, matched_pattern="database_scope"))
        seen_urns.add(urn)

    return candidates


def delete_entities(
    graph: DataHubGraph,
    entities: list[CandidateEntity],
    hard_delete: bool,
) -> tuple[int, int]:
    success_count = 0
    failure_count = 0
    delete_mode = "hard" if hard_delete else "soft"

    for entity in entities:
        logger.info(
            "Deleting entity urn=%s matched_by=%s mode=%s",
            entity.urn,
            entity.matched_pattern,
            delete_mode,
        )
        try:
            graph.delete_entity(entity.urn, hard=hard_delete)
            success_count += 1
            logger.info("Deleted entity urn=%s mode=%s", entity.urn, delete_mode)
        except Exception:
            logger.exception("Failed to delete %s", entity.urn)
            failure_count += 1

    return success_count, failure_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Find entities that match EXCLUDED_NAME_PATTERNS and delete them from DataHub. "
            "By default this is a dry run."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletion. If omitted, script runs in dry-run mode.",
    )
    parser.add_argument(
        "--hard-delete",
        action="store_true",
        help="Hard delete entities. Default is soft delete.",
    )
    parser.add_argument(
        "--entity-types",
        nargs="+",
        default=["dataset"],
        help=(
            "Entity types to search, e.g. dataset container chart dashboard. "
            "Default: dataset"
        ),
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Optional platform filter, e.g. dbt, glue, postgres.",
    )
    parser.add_argument(
        "--env",
        default=None,
        help="Optional DataHub env filter, e.g. PROD, DEV.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Search page size for scroll query.",
    )
    parser.add_argument(
        "--max-entities",
        type=int,
        default=500,
        help=(
            "Safety limit. If candidate count exceeds this, script aborts unless --force is provided."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow processing even if candidate count exceeds --max-entities.",
    )
    parser.add_argument(
        "--extra-patterns",
        nargs="*",
        default=[],
        help="Additional lowercase substrings to include in matching.",
    )
    parser.add_argument(
        "--show-sample",
        type=int,
        default=25,
        help="How many candidate URNs to print.",
    )
    parser.add_argument(
        "--require-display-tag",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Require entities to have urn:li:tag:dc_display_in_catalogue. "
            "Use --no-require-display-tag to disable."
        ),
    )
    parser.add_argument(
        "--target-urns",
        nargs="*",
        default=[],
        help=(
            "Explicit list of URNs to include for deletion. "
            "These are included regardless of name-pattern matching."
        ),
    )
    parser.add_argument(
        "--dataset-database",
        default=None,
        help=(
            "Optional dataset database name to scope deletion to, e.g. dlpes_dfe_datashare."
        ),
    )
    parser.add_argument(
        "--keep-table-prefix",
        default=None,
        help=(
            "Optional table prefix to keep. Any dataset in --dataset-database whose table name starts with this prefix is skipped."
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
    datahub_gms_token = os.getenv("DATAHUB_GMS_TOKEN")

    if not datahub_gms_url or not datahub_gms_token:
        raise ValueError(
            "Missing required environment variables DATAHUB_GMS_URL and/or DATAHUB_GMS_TOKEN"
        )

    server_config = DatahubClientConfig(
        server=datahub_gms_url,
        token=datahub_gms_token,
    )
    graph = DataHubGraph(server_config)

    patterns = list(dict.fromkeys(list(EXCLUDED_NAME_PATTERNS) + args.extra_patterns))
    explicit_urns = [urn.strip() for urn in args.target_urns if urn and urn.strip()]

    logger.info("Using patterns: %s", ", ".join(patterns))
    logger.info("Explicit target URNs: %d", len(explicit_urns))
    logger.info("Entity types: %s", ", ".join(args.entity_types))
    logger.info("Platform filter: %s", args.platform or "<none>")
    logger.info("Env filter: %s", args.env or "<none>")
    logger.info("Require display tag: %s", args.require_display_tag)
    logger.info("Dataset database scope: %s", args.dataset_database or "<none>")
    logger.info("Keep table prefix: %s", args.keep_table_prefix or "<none>")

    pattern_candidates = find_candidates(
        graph=graph,
        patterns=patterns,
        entity_types=args.entity_types,
        batch_size=args.batch_size,
        platform=args.platform,
        env=args.env,
        require_display_tag=args.require_display_tag,
        dataset_database=args.dataset_database,
        keep_table_prefix=args.keep_table_prefix,
    )

    database_scope_candidates = find_database_scope_candidates(
        graph=graph,
        batch_size=args.batch_size,
        platform=args.platform,
        env=args.env,
        require_display_tag=args.require_display_tag,
        dataset_database=args.dataset_database,
        keep_table_prefix=args.keep_table_prefix,
    )

    candidates_by_urn: dict[str, CandidateEntity] = {
        candidate.urn: candidate
        for candidate in [*pattern_candidates, *database_scope_candidates]
    }
    for urn in explicit_urns:
        if is_protected_urn(urn):
            logger.info("Skipping explicit protected platform entity urn=%s", urn)
            continue

        if not is_dataset_in_scope(
            urn,
            dataset_database=args.dataset_database,
            keep_table_prefix=args.keep_table_prefix,
        ):
            logger.info("Skipping explicit out-of-scope dataset urn=%s", urn)
            continue

        candidates_by_urn.setdefault(
            urn,
            CandidateEntity(urn=urn, matched_pattern="explicit_urn"),
        )

    candidates = list(candidates_by_urn.values())

    pattern_counts: dict[str, int] = defaultdict(int)
    for candidate in candidates:
        pattern_counts[candidate.matched_pattern] += 1

    logger.info(
        "Found %d candidate entities (pattern=%d database_scope=%d explicit=%d)",
        len(candidates),
        len(pattern_candidates),
        len(database_scope_candidates),
        len(explicit_urns),
    )
    for pattern, count in sorted(pattern_counts.items()):
        logger.info("  pattern=%s count=%d", pattern, count)

    for candidate in candidates[: args.show_sample]:
        logger.info("sample urn=%s", candidate.urn)

    if len(candidates) > args.max_entities and not args.force:
        logger.error(
            "Aborting because %d candidates exceed --max-entities=%d. Re-run with --force if intentional.",
            len(candidates),
            args.max_entities,
        )
        return 2

    if not args.apply:
        logger.info("Dry run complete. Re-run with --apply to perform deletion.")
        return 0

    success_count, failure_count = delete_entities(
        graph=graph,
        entities=candidates,
        hard_delete=args.hard_delete,
    )

    logger.info(
        "Deletion finished. success=%d failure=%d mode=%s",
        success_count,
        failure_count,
        "hard" if args.hard_delete else "soft",
    )

    return 1 if failure_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
