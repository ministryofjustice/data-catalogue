import json
import logging
import os
import re
from enum import StrEnum
from typing import Dict, Tuple

import boto3
import datahub.emitter.mce_builder as builder
import datahub.emitter.mce_builder as mce_builder
from botocore.exceptions import ClientError, NoCredentialsError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    CorpUserInfoClass,
    DomainPropertiesClass,
)

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.utils import report_time

logging.basicConfig(level=logging.DEBUG)


class FindMojDataEntityTypes(StrEnum):
    """
    An enum to hold subtypes specific to moj data that are outside
    of the datahub standard subtype offerings
    """

    # to be used as container subtype
    PUBLICATION_COLLECTION = "Publication collection"
    # to be used as dataset subtype
    PUBLICATION_DATASET = "Publication dataset"


@report_time
def get_cadet_metadata_json(s3_uri: str) -> Dict:
    """
    Returns dict object containing metadata from the json file at the given s3 path.
    Examples are the manifest file or the database_metadata file
    """
    try:
        s3 = boto3.client("s3")
        s3_parts = s3_uri.split("/")
        bucket_name = s3_parts[2]
        file_key = "/".join(s3_parts[3:])
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        metadata = json.loads(content, strict=False)
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
    return metadata


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
    node_database_name = node["schema"]

    return node_database_name, node_table_name


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


def get_tags(dbt_manifest_node: dict) -> list[str]:
    """Resolve the tags to assign to nodes in datahub."""
    tags = []
    if "dc_display_in_catalogue" in dbt_manifest_node["tags"]:
        tags.append("dc_display_in_catalogue")
    if dbt_manifest_node["resource_type"] == "seed":
        tags.append("dc_display_in_catalogue")

    return tags


def make_user_mcp(email: str) -> MetadataChangeProposalWrapper:
    if not email.endswith(".gov.uk"):
        email = email + "@justice.gov.uk"
    user_urn = mce_builder.make_user_urn(email.split("@")[0])

    user_info = CorpUserInfoClass(
        active=False,
        displayName=email.split("@")[0].replace(".", " "),
        email=email,
    )
    user_mcp = MetadataChangeProposalWrapper(
        entityType="corpuser",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=user_urn,
        aspect=user_info,
    )

    return user_mcp


def make_domain_mcp(domain_name: str) -> MetadataChangeProposalWrapper:
    domain_urn = mce_builder.make_domain_urn(domain=domain_name)
    domain_properties = DomainPropertiesClass(name=domain_name)
    mcp = MetadataChangeProposalWrapper(
        entityType="domain",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=domain_urn,
        aspect=domain_properties,
    )
    return mcp
