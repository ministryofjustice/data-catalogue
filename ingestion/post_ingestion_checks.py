import argparse
import json
import logging
import os

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

logging.basicConfig(level=logging.INFO)

logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.INFO)

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
    # mappings is a dictionary where the key is the dataset urn and the value is the database urn
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
        if not relations:
            missing_is_part_of.append(dataset)
    return missing_is_part_of


def create_query_input(platform: str) -> dict:
    return {
        "input": {
            "types": [],
            "facets": ["_entityType", "tags", "owners"],
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


def parse_count_by_platform_results(result: dict):
    return {
        field["field"]: field["aggregations"]
        for field in result["aggregateAcrossEntities"]["facets"]
    }


def counts_by_platform(env: str, platforms: list, graph: DataHubGraph):
    query_results = {}
    for platform in platforms:
        result = graph.execute_graphql(
            count_by_platform_graph_query, create_query_input(platform)
        )

        query_results[platform] = parse_count_by_platform_results(result)

    with open(os.environ["GITHUB_OUTPUT"], "a") as output_file:
        output_file.write(f"{env.lower()}_results={json.dumps(query_results)}\n")


def relations_check(s3_manifest_path: str, graph: DataHubGraph):
    manifest = get_cadet_metadata_json(s3_manifest_path)

    mappings = _get_table_database_mappings(manifest)

    missing_is_part_of = check_is_part_of_relationships(mappings, graph)

    print(f"::set-output name=missing_is_part_of::{json.dumps(missing_is_part_of)}")


def _calculate_missing_values(prod_values: set, preprod_values: set) -> dict:
    return {
        "missing_in_preprod": list(prod_values - preprod_values),
        "missing_in_prod": list(preprod_values - prod_values),
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
) -> dict:
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

    return {
        "missing_values": _remove_empty_dicts(missing_values),
        "mismatched_counts": _remove_empty_dicts(mismatched_counts),
    }


FUNCTION_MAP = {
    "relations": relations_check,
    "counts": counts_by_platform,
    "compare": compare_environment_counts,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=FUNCTION_MAP.keys(),
        help="indicates which check to run",
    )
    parser.add_argument("--env", required=False, help="Environment being queried")
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

    if args.command == "counts":
        server_config = DatahubClientConfig(
            server=os.environ["DATAHUB_GMS_URL"], token=os.environ["DATAHUB_GMS_TOKEN"]
        )
        graph = DataHubGraph(server_config)
        FUNCTION_MAP[args.command](env=args.env, platforms=args.platforms, graph=graph)
    elif args.command == "relations":
        server_config = DatahubClientConfig(
            server=os.environ["DATAHUB_GMS_URL"], token=os.environ["DATAHUB_GMS_TOKEN"]
        )
        graph = DataHubGraph(server_config)
        FUNCTION_MAP[args.command](s3_manifest_path=args.s3_manifest_path, graph=graph)
    elif args.command == "compare":
        comparison_results = FUNCTION_MAP[args.command](
            platforms=args.platforms,
            prod_results=json.loads(args.prod_results),
            preprod_results=json.loads(args.preprod_results),
        )
        print(f"::set-output name=comparison_results::{json.dumps(comparison_results)}")
