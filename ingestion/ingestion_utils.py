import json
import logging
import os
import re
from typing import Dict, Tuple

import boto3
import datahub.emitter.mce_builder as builder
from botocore.exceptions import ClientError, NoCredentialsError
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.utils import report_time

logging.basicConfig(level=logging.DEBUG)


@report_time
def get_cadet_manifest(manifest_s3_uri: str) -> Dict:
    try:
        s3 = boto3.client("s3")
        s3_parts = manifest_s3_uri.split("/")
        bucket_name = s3_parts[2]
        file_key = "/".join(s3_parts[3:])
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        manifest = json.loads(content, strict=False)
    except NoCredentialsError:
        print("Credentials not available.")
        raise
    except ClientError as e:
        # If a client error is thrown, it will have a response attribute containing the error details
        error_code = e.response["Error"]["Code"]
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


def validate_fqn(fqn: list[str]) -> bool:
    """The table name for CaDeT models should be of form {database}__{table}"""
    table_name = fqn[-1]

    double_underscores = re.findall("__", table_name)
    if len(double_underscores) > 1:
        logging.warning(
            f"{table_name=} has multiple double underscores which will confuse parsing"
        )

    match: re.Match[str] | None = re.match(r"\w+__\w+", table_name)
    if match:
        return True
    if not match:
        logging.warning(f"{table_name=} does not match database__table format")
        return False


def convert_cadet_manifest_table_to_datahub(node_info: dict) -> Tuple[str, str]:
    """
    eg 'database__table' is converted to a regex string to detect it's urn
    like 'urn:li:dataset:\\(urn:li:dataPlatform:dbt,cadet\\.awsdatacatalog\\.database\\.table,PROD\\)'
    """
    domain = format_domain_name(node_info.get("fqn", [])[1])

    database_name, table_name = parse_database_and_table_names(node_info)

    urn = builder.make_dataset_urn_with_platform_instance(
        platform=PLATFORM,
        platform_instance=INSTANCE,
        env=ENV,
        name=f"{database_name}.{table_name}",
    )
    escaped_urn_for_regex = re.escape(urn)

    return domain, escaped_urn_for_regex


BIG_OLD_ACRONYMS = set(("OPG", "HMPPS", "HMCTS", "LAA", "CICA", "HQ"))


def format_domain_name(domain_name: str) -> str:
    """
    Format domain names from the manifest into a human readable format, e.g.
    courts -> Courts
    opg -> OPG
    hmpps -> HMPPS
    electronic_monitoring -> Electronic monitoring
    """
    acronym = domain_name.upper()
    if acronym in BIG_OLD_ACRONYMS:
        return acronym

    return domain_name.capitalize().replace("_", " ")


def parse_database_and_table_names(node: dict) -> tuple[str, str]:
    """
    takes a node from the dbt manifest and returns the athena database
    and table names - as populated by the create-a-derived-table service
    """

    # In CaDeT the convention is to name a table database__table, which is
    # found in the last item of fqn list.
    node_table_name = node["fqn"][-1].split("__")[-1]
    # schema holds the database name after parsing from cadet and so will be
    # representative of the cadet env (where dev dbs have suffix `_dev_dbt`)
    node_dataabse_name = node["schema"]

    return node_dataabse_name, node_table_name


def list_datahub_domains() -> list[str]:
    """
    Returns a list of domains as exists in datahub
    """
    server_config = DatahubClientConfig(
        server=os.environ["DATAHUB_GMS_URL"], token=os.environ["DATAHUB_GMS_TOKEN"]
    )

    graph = DataHubGraph(server_config)

    list_domains_query = """
        {listDomains(
            input: {start: 0, count: 50}
        ) {
        domains{
            urn
            properties{
                name
            }
            entities(
            input:{query:"*",start:0,count: 0}
            ){
                total
            }
        }
        }
        }
        """
    results = graph.execute_graphql(list_domains_query)

    domains_list = [
        domain["properties"]["name"].lower()
        for domain in results["listDomains"]["domains"]
    ]
    return domains_list
