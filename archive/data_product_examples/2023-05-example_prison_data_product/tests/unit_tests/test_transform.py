from application.transform import generate_report, get_tables
from unittest.mock import patch
import pandas as pd
import pytest
from pathlib import Path
import os


@pytest.fixture
def bucket_name():
    return "product-test-bucket"


@pytest.fixture(scope="function")
def s3_test_bucket(s3_mock_client, bucket_name):
    s3_mock_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    yield


data_product_directory = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data"
)

sample_input = pd.read_csv(os.path.join(
    data_product_directory, "sample_input.csv"))


table_dict = {"product_for_test": ["adj_example_1"]}


@patch(
    "application.transform.get_data", return_value=pd.DataFrame.from_dict(sample_input)
)
@patch("application.transform.get_tables", return_value=table_dict)
def test_generate_report(mocked_data, mocked_tables):
    data_product_directory = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "data"
    )
    sample_output = pd.read_csv(
        os.path.join(data_product_directory, "sample_output.csv")
    )
    pd.testing.assert_frame_equal(
        generate_report("fake_bucket", "product_for_test/test_table1/data.csv")[
            "product_for_test"
        ]["adj_example_1"],
        sample_output,
    )


def test_get_tables(s3_mock_client, s3_test_bucket):
    s3_mock_client.upload_file(
        os.path.join(
            Path(__file__).parent.absolute(
            ), "test_metadata", "02-data-dictionary.yaml"
        ),
        "product-test-bucket",
        "code/product_for_test/extracted/metadata/02-data-dictionary.yml",
    )

    expected_tables = {"product_for_test": ["test_table1"]}

    tables = get_tables(
        bucket="product-test-bucket",
        key="raw_data/product_for_test/test_table1/data.csv",
        source_data="test1",
    )
    assert tables == expected_tables
