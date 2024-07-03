import json
import logging
import re
from typing import Dict, Tuple

import boto3
import datahub.emitter.mce_builder as builder
from botocore.exceptions import ClientError, NoCredentialsError

from ingestion.config import ENV, INSTANCE, PLATFORM


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

    match = re.match(r"\w+__\w+", table_name)
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
    node_table_name = node_info.get("fqn", [])[-1]

    # In CaDeT the convention is to name a table database__table
    node_table_name_no_double_underscore = node_table_name.replace("__", ".")
    urn = builder.make_dataset_urn_with_platform_instance(
        platform=PLATFORM,
        platform_instance=INSTANCE,
        env=ENV,
        name=node_table_name_no_double_underscore,
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
