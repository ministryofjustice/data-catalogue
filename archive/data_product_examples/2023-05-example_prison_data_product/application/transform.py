import pandas as pd
import logging
import boto3
import yaml
import os
from pathlib import Path
from typing import Dict

logging.basicConfig()


def get_data(bucket: str, key: str) -> pd.DataFrame:
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        logging.info("Data has been collected from {}/{}".format(bucket, key))
        return pd.read_csv(response.get("Body"))
    else:
        logging.error(
            "Bucket {} or file {} does not exist".format(bucket, key))
        raise


def get_tables(
    bucket: str, key: str, source_data: str
) -> Dict[str, list[str]]:
    s3_client = boto3.client("s3")
    product_name = Path(key).parts[1]
    yaml_key = os.path.join(
        "code", product_name, "extracted", "metadata", "02-data-dictionary.yml"
    )
    response = s3_client.get_object(Bucket=bucket, Key=yaml_key)
    data_dict = yaml.safe_load(response["Body"])
    tables_dict = {}
    for key in data_dict:
        tables_dict[key] = [
            table for table in data_dict[key]['tables']
            if data_dict[key]['tables'][table]['source_data'] == source_data
        ]

    return tables_dict


def generate_report(bucket: str, key: str) -> Dict[str, pd.DataFrame]:
    results_dict = {}
    source_data = Path(key).parts[2]
    data_products_dict = get_tables(bucket, key, source_data)
    raw_data = get_data(bucket, key)
    for database, tables in data_products_dict.items():
        results_dict[database] = {}
        for table in tables:
            # e.g. One loop of this might evaluate to `table_1(bucket, key, data)`
            # and execute one of the functions below with the correct arguments
            results_dict[database][table] = eval(
                table + "(bucket, key, raw_data)"
            )

    return results_dict


# Add functions for each table transformation below
# The names of each function need to match up with the tables
# as defined in the metadata data-dictionary.yml file

# function to create table 1
def adj_example_1(
    bucket: str,
    key: str,
    raw_data: pd.DataFrame
) -> pd.DataFrame:
    # group by establishment, religion, offence and get count offence
    transformed_data = raw_data.value_counts(
        subset=["Establishment", "Religion", "Offence"], sort=False).reset_index()
    transformed_data.columns = [
        "Establishment", "Religion", "Offence", "Count"]

    logging.info("Data is transformed")
    return transformed_data


# function to create table 2
def adj_example_2(
    bucket: str,
    key: str,
    raw_data: pd.DataFrame
) -> pd.DataFrame:

    transformed_data = raw_data.value_counts(
        subset=["Establishment"], sort=False).reset_index()
    transformed_data.columns = [
        "Establishment", "Count"]

    logging.info("Data is transformed")
    return transformed_data


def punishments_example_1(
    bucket: str,
    key: str,
    raw_data: pd.DataFrame
) -> pd.DataFrame:

    transformed_data = raw_data.value_counts(
        subset=["Establishment"], sort=False).reset_index()
    transformed_data.columns = [
        "Establishment", "Count"]

    logging.info("Data is transformed")
    return transformed_data
