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
    r"^urn:li:dataset:\\(urn:li:dataPlatform:[^,]+,([^,]+),[^\\)]+\\)$"
)
CONTAINER_URN_PATTERN = re.compile(r"^urn:li:container:(.+)$")


@dataclass(frozen=True)
class CandidateEntity:
    urn: str
    matched_pattern: str


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
        return dataset_match.group(1)

    container_match = CONTAINER_URN_PATTERN.match(urn)
    if container_match:
        return container_match.group(1)

    return urn


def find_candidates(
    graph: DataHubGraph,
    patterns: Iterable[str],
    entity_types: list[str],
    batch_size: int,
    platform: str | None,
    env: str | None,
    require_display_tag: bool,
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

    def iter_matching_urns(pattern_query: str):
        # Containers often do not have an env facet. If env filtering is enabled,
        # search containers without env so database containers are still found.
        if env and "container" in entity_types:
            non_container_types = [et for et in entity_types if et != "container"]

            if non_container_types:
                for urn in graph.get_urns_by_filter(
                    entity_types=non_container_types,
                    query=pattern_query,
                    batch_size=batch_size,
                    platform=platform,
                    env=env,
                    status=RemovedStatusFilter.NOT_SOFT_DELETED,
                    extraFilters=extra_filters,
                ):
                    yield urn

            for urn in graph.get_urns_by_filter(
                entity_types=["container"],
                query=pattern_query,
                batch_size=batch_size,
                platform=platform,
                env=None,
                status=RemovedStatusFilter.NOT_SOFT_DELETED,
                extraFilters=extra_filters,
            ):
                yield urn

            return

        for urn in graph.get_urns_by_filter(
            entity_types=entity_types,
            query=pattern_query,
            batch_size=batch_size,
            platform=platform,
            env=env,
            status=RemovedStatusFilter.NOT_SOFT_DELETED,
            extraFilters=extra_filters,
        ):
            yield urn

    for pattern in patterns:
        query = pattern
        logger.info("Searching for entities with query=%s", query)

        for urn in iter_matching_urns(query):
            if urn in seen_urns:
                continue

            # DataHub full-text search can return broad matches for short
            # patterns (for example, "int"). For dataset entities, enforce a
            # strict substring check against the parsed dataset name.
            if urn.startswith("urn:li:dataset:"):
                parsed_name = parse_name_from_urn(urn).lower()
                if pattern not in parsed_name:
                    continue

            if urn.startswith("urn:li:container:"):
                if urn not in container_name_cache:
                    container_name_cache[urn] = get_container_display_name(graph, urn)

                display_name = container_name_cache[urn]
                if not display_name:
                    # Without display name, skip to avoid false positives.
                    continue

                if pattern not in display_name.lower():
                    continue

            # Trust DataHub's full-text search match for the query pattern.
            # This avoids false negatives for entities like containers where
            # the URN does not contain the display name.
            candidates.append(CandidateEntity(urn=urn, matched_pattern=pattern))
            seen_urns.add(urn)

    return candidates


def delete_entities(
    graph: DataHubGraph,
    entities: list[CandidateEntity],
    hard_delete: bool,
) -> tuple[int, int]:
    success_count = 0
    failure_count = 0

    for entity in entities:
        try:
            graph.delete_entity(entity.urn, hard=hard_delete)
            success_count += 1
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

    logger.info("Using patterns: %s", ", ".join(patterns))
    logger.info("Entity types: %s", ", ".join(args.entity_types))
    logger.info("Platform filter: %s", args.platform or "<none>")
    logger.info("Env filter: %s", args.env or "<none>")
    logger.info("Require display tag: %s", args.require_display_tag)

    candidates = find_candidates(
        graph=graph,
        patterns=patterns,
        entity_types=args.entity_types,
        batch_size=args.batch_size,
        platform=args.platform,
        env=args.env,
        require_display_tag=args.require_display_tag,
    )

    pattern_counts: dict[str, int] = defaultdict(int)
    for candidate in candidates:
        pattern_counts[candidate.matched_pattern] += 1

    logger.info("Found %d candidate entities", len(candidates))
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
