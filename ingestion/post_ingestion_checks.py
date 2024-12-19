import argparse
import json
import os
import time

import datahub.emitter.mce_builder as mce_builder
import datahub.emitter.mcp_builder as mcp_builder
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.ingestion_utils import (
    get_cadet_metadata_json,
    parse_database_and_table_names,
    validate_fqn,
)

count_by_platform_graph_query = """
query platformCounts($input: AggregateAcrossEntitiesInput!) {
    aggregateAcrossEntities(input: $input) {
        facets {
            field
            displayName
            aggregations {
                value
                count
            }
        }
    }
}
"""


def _get_table_database_mappings(manifest):
    mappings = {}
    for node in manifest["nodes"]:
        if manifest["nodes"][node]["resource_type"] in ["model", "seed"]:
            fqn = manifest["nodes"][node]["fqn"]
            if validate_fqn(fqn):
                database, table_name = parse_database_and_table_names(
                    manifest["nodes"][node]
                )

                dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
                    name=f"{database}.{table_name}",
                    platform=PLATFORM,
                    platform_instance=INSTANCE,
                    env=ENV,
                )
                database_key = mcp_builder.DatabaseKey(
                    database=database,
                    platform=PLATFORM,
                    instance=INSTANCE,
                    env=ENV,
                    backcompat_env_as_instance=True,
                )
                database_urn = database_key.as_urn()
                if "dc_display_in_catalogue" in manifest["nodes"][node]["tags"]:
                    mappings[dataset_urn] = database_urn

    return mappings


def _remove_empty_dicts(d):
    if not isinstance(d, dict):
        return d
    cleaned_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            nested = _remove_empty_dicts(v)
            if nested:  # Only add non-empty dictionaries
                cleaned_dict[k] = nested
        elif v:  # Only add non-empty values
            cleaned_dict[k] = v
    return cleaned_dict


def check_is_part_of_relationships(mappings, graph):
    """
    this checks whether datasets from cadet have the ispartof relationship
    to their container. Without this fmd containers appear empty.

    returns a list of datasets where this relationship is missing
    """

    missing_is_part_of = []

    for dataset in mappings:
        relations = list(
            graph.get_related_entities(
                dataset,
                ["IsPartOf"],
                DataHubGraph.RelationshipDirection.OUTGOING,
            )
        )
        if len(relations) == 0:
            missing_is_part_of.append(dataset)
    return missing_is_part_of


def create_query_input(platform: str) -> dict:
    query_input = {
        "input": {
            "types": [],
            "facets": ["_entityType", "tags", "domains", "owners"],
            "orFilters": [
                {
                    "and": [
                        {
                            "field": "platform",
                            "condition": "EQUAL",
                            "values": [f"urn:li:dataPlatform:{platform}"],
                        }
                    ]
                }
            ],
            "query": "",
            "searchFlags": {"maxAggValues": 2000},
        }
    }
    return query_input


def parse_count_by_platform_results(result: dict):
    parsed_result = {
        field["field"]: field["aggregations"]
        for field in result["aggregateAcrossEntities"]["facets"]
    }
    return parsed_result


def counts_by_platform(env: str, platforms: list):
    query_results = {}
    for platform in platforms:
        result = graph.execute_graphql(
            count_by_platform_graph_query, create_query_input(platform)
        )

        query_results[platform] = parse_count_by_platform_results(result)

    print(f"::set-output name={env.lower()}_results::{json.dumps(query_results)}")


def relations_check(s3_manifest_path: str, graph: DataHubGraph):
    manifest = get_cadet_metadata_json(s3_manifest_path)

    mappings = _get_table_database_mappings(manifest)

    missing_is_part_of = check_is_part_of_relationships(mappings, graph)

    print(f"::set-output name=missing_is_part_of::{json.dumps(missing_is_part_of)}")

    return None


def _calculate_missing_values(prod_values: set, preprod_values: set) -> dict:
    return {
        "missing_in_preprod": prod_values - preprod_values,
        "missing_in_prod": preprod_values - prod_values,
    }


def _calculate_mismatched_counts(
    prod_counts: dict, preprod_counts: dict, threshold: float
) -> dict:
    mismatched = {}
    for aspect, count in prod_counts.items():
        preprod_count = preprod_counts.get(aspect, 0)
        if count != 0 and preprod_count != 0:
            ratio = preprod_count / count
            if abs(1 - ratio) > threshold:
                mismatched[aspect] = ratio
    return mismatched


def compare_environment_counts(
    platforms: list,
    prod_results: dict,
    preprod_results: dict,
    mismatch_threshold: float = 0.2,
) -> tuple[dict, dict]:
    missing_values = {}
    mismatched_counts = {}

    for platform in platforms:
        missing_values[platform] = {}
        mismatched_counts[platform] = {}

        for field in prod_results.get(platform, {}):
            # Extract values and counts from prod and preprod results
            prod_field_data = prod_results[platform].get(field, [])
            preprod_field_data = preprod_results[platform].get(field, [])

            prod_values = {item["value"] for item in prod_field_data}
            preprod_values = {item["value"] for item in preprod_field_data}
            prod_counts = {item["value"]: item["count"] for item in prod_field_data}
            preprod_counts = {
                item["value"]: item["count"] for item in preprod_field_data
            }

            # Calculate missing values
            if prod_values != preprod_values:
                missing_values[platform][field] = _calculate_missing_values(
                    prod_values, preprod_values
                )

            # Calculate mismatched counts
            mismatched_counts[platform][field] = _calculate_mismatched_counts(
                prod_counts, preprod_counts, mismatch_threshold
            )

    return _remove_empty_dicts(missing_values), _remove_empty_dicts(mismatched_counts)


FUNCTION_MAP = {
    "relations": relations_check,
    "counts": counts_by_platform,
    "compare": compare_environment_counts,
}

if __name__ == "__main__":
    s = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=FUNCTION_MAP.keys(),
        help="indicates which check to run",
    )
    parser.add_argument("--env", required=True, help="Environment being queried")
    parser.add_argument(
        "--platforms",
        nargs="+",
        required=False,
        help="a list of platforms to group count query results by",
    )
    parser.add_argument(
        "--s3-manifest-path",
        required=False,
        help="path to the dbt manifest file in s3",
    )
    parser.add_argument(
        "--prod-results",
        required=False,
        help="results from prod count query",
    )
    parser.add_argument(
        "--preprod-results",
        required=False,
        help="results from preprod count query",
    )
    args = parser.parse_args()
    server_config = DatahubClientConfig(
        server=os.environ["DATAHUB_GMS_URL"], token=os.environ["DATAHUB_GMS_TOKEN"]
    )
    graph = DataHubGraph(server_config)
    if args.command == "counts":
        FUNCTION_MAP[args.command](env=args.env, platforms=args.platforms)
    elif args.command == "relations":
        FUNCTION_MAP[args.command](s3_manifest_path=args.s3_manifest_path)
    elif args.command == "compare":
        FUNCTION_MAP[args.command](
            prod_results=args.prod_results,
            preprod_results=args.preprod_results,
        )
