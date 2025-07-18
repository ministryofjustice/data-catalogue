import logging
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
from datetime import date, datetime, UTC, timedelta

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

HOST: str = os.getenv("DATAHUB_GMS_URL", "").replace("/api/gms", "")
TOKEN: str = os.getenv("DATAHUB_GMS_TOKEN", "")
logging.info(f"Using host {HOST}")


def get_time_delta(days: int) -> date:
    return (datetime.now(UTC) - timedelta(days=days)).date()


def get_graph_client() -> DataHubGraph:
    return DataHubGraph(
        config=DatahubClientConfig(
            server=HOST,
            token=TOKEN,
        )
    )


def get_graph_variables() -> dict[str, Any]:
    return {
        "count": 1000,
        "query": "*",
        "start": 0,
        "types": ["CONTAINER", "DATASET", "CHART", "DASHBOARD"],
        "filters": [{"and": [{"field": "removed", "values": ["true"]}]}],
    }


def get_graphql_query(query_file_path: Path):
    with open(query_file_path) as f:
        query = f.read()
        return query


def get_graph_response(
    graph_client: DataHubGraph, graph_query: str, graph_variables: dict[str, Any]
) -> dict[str, dict[str, Any]] | None:
    try:
        response = graph_client.execute_graphql(
            query=graph_query, variables=graph_variables
        )
    except Exception as e:
        logging.exception(
            f"Error making graphql with query {graph_query} and variables {graph_variables}"
        )
        logging.exception(str(e))
        return None
    else:
        return response


def get_soft_deleted_entities(
    response: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    search_results = response.get("searchAcrossEntities", {}).get("searchResults", [])
    return search_results


def filter_soft_deleted_entities_by_timestamp(
    search_results: list[dict], days: int = 7
) -> list[dict[str, dict[str, str]]]:
    """filters soft deleted entities older than the number of days.

    Args:
        search_results (list[dict]): list of softdeleted entities
        days (int, optional): The number of days to filter againts. Defaults to 7.

    Returns:
        list[dict[str, str]]: A list of soft deleted entities,
          with a lastIngested timestamp older than the configured days.
    """
    time_delta = get_time_delta(days=days)
    logging.info(f"filtering entities older than {time_delta}")
    filtered_results: list[dict[str, str]] = []
    for result in search_results:
        last_ingested = result.get("entity", {}).get("lastIngested", 0)
        entity_date = datetime.fromtimestamp(last_ingested / 1e3, UTC).date()
        if time_delta > entity_date:
            filtered_results.append(result)
    return filtered_results


def perform_hard_delete_on_entities(
    filtered_results: list[dict[str, dict[str, str]]],
) -> None:
    for result in filtered_results:
        urn: str = result["entity"]["urn"]
        entity_type: str = result["entity"]["type"]
        command: list[str] = [
            "uv",
            "run",
            "datahub",
            "delete",
            "--urn",
            f"{urn}",
            "--force",
        ]

        if entity_type == "CONTAINER":
            command.append("--recursive")

        command_string = " ".join(command)
        logging.info(f"deleting with command '{command_string}'")

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,  # This will raise CalledProcessError if the command fails
            )
            logging.info(result.stdout)
        except subprocess.CalledProcessError as e:
            logging.exception(f"Error running datahub delete: {e}\nStderr: {e.stderr}")
        except Exception as e:
            logging.exception(f"Unexpected error: {str(e)}")
        time.sleep(0.5)


def main():
    script_dir: Path = Path(__file__).parent
    graphql_query_path: Path = script_dir / "graphql" / "search.graphql"
    graph_client = get_graph_client()
    graph_query = get_graphql_query(graphql_query_path)
    graph_variables = get_graph_variables()
    response = get_graph_response(
        graph_client=graph_client,
        graph_query=graph_query,
        graph_variables=graph_variables,
    )

    search_results = get_soft_deleted_entities(response=response)
    if not search_results:
        logging.info("No soft deleted entities returned, exiting")
        sys.exit(0)
    logging.info(f"Soft deleted results returned {len(search_results)}")
    filtered_results = filter_soft_deleted_entities_by_timestamp(
        search_results=search_results
    )
    logging.info(f"Filtered soft deleted results {len(filtered_results)}")

    perform_hard_delete_on_entities(filtered_results=filtered_results)


if __name__ == "__main__":
    main()
