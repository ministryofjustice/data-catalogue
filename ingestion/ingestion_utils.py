import json
import logging
import os
import re
from enum import StrEnum
from typing import Dict, Generic, TypeVar

import boto3
import datahub.emitter.mce_builder as mce_builder
import yaml
from botocore.exceptions import ClientError, NoCredentialsError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ChangeTypeClass, CorpUserInfoClass

from ingestion.utils import report_time

logging.basicConfig(level=logging.DEBUG)

# This is so we can quickly tag entities with subject areas to test, before
# adding the tags at source. Domains relate to those used in CaDeT.
# bold and general do not map to any subject area and will need to be handled
# when/if we make the changes at source
domains_to_subject_areas = {
    "bold": None,
    "civil": "Courts and tribunals",
    "courts": "Courts and tribunals",
    "electronic monitoring": "Prisons and probation",
    "finance": "Corporate operations",
    "general": None,
    "interventions": "Prisons and probation",
    "opg": "Office of the Public Guardian",
    "people": "Corporate operations",
    "prison": "Prisons and probation",
    "probation": "Prisons and probation",
    "property": "Corporate operations",
    "risk": "Prisons and probation",
}


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


ValueType = TypeVar("ValueType")


class NodeLookup(Generic[ValueType]):
    """
    Container class for storing values associated with a node.
    Values can be fetched by database and table, or iterated over.
    """

    def __init__(self):
        self.database_lookup = {}
        self.table_lookup = {}

    def get(self, database: str, table: str = "") -> ValueType:
        """
        Get the value associated with a database/table.
        If table is ommitted, return the value from the last added
        node matching the database.
        """
        if table:
            return self.table_lookup[(database, table)]
        else:
            return self.database_lookup[database]

    def set(self, database: str, table: str, value: ValueType):
        self.database_lookup[database] = value
        self.table_lookup[(database, table)] = value

    def __iter__(self):
        return (
            (database, table, domain)
            for ((database, table), domain) in self.table_lookup.items()
        )


def get_subject_areas():
    """
    Returns a list of top level subject areas from the subject_areas_template.yaml file
    """
    subject_areas_filepath = os.path.join(
        os.path.dirname(__file__), "tags", "top_level_subject_areas_template.yaml"
    )
    with open(subject_areas_filepath, "r") as file:
        subject_areas_yaml = yaml.safe_load(file)
        top_level_subject_areas = [item["name"] for item in subject_areas_yaml]

    return top_level_subject_areas
