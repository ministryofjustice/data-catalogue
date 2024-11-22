from datetime import datetime, timedelta, timezone
import os
from unittest.mock import patch, mock_open
import yaml

from ingestion.cadet_run_results import (
    get_cadet_run_result_paths,
    inject_run_result_paths_into_yaml_template,
)

yaml_data = {
    "source": {
        "type": "dbt",
        "config": {
            "aws_connection": {"aws_region": "eu-west-1"},
            "manifest_path": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json",
            "catalog_path": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/catalog.json",
            "run_results_paths": [
                "s3://mojap-derived-tables/prod/run_artefacts/latest/target/run_results.json"
            ],
        },
    }
}


@patch("ingestion.cadet_run_results.get_cadet_run_result_paths")
def test_inject_run_results_into_yaml_template(
    mock_cadet_run_result_paths,
    cadet_test_recipe_path="tests/data/cadet_test_recipe_file.yaml",
):
    mock_cadet_run_result_paths.return_value = [
        "s3://mojap-derived-tables/prod/run_artefacts/run_results.json",
        "s3://mojap-derived-tables/prod/run_artefacts/123/run_results.json",
    ]
    with open(cadet_test_recipe_path, "w") as f:
        yaml.dump(yaml_data, f, indent=2, sort_keys=False, default_flow_style=False)

    inject_run_result_paths_into_yaml_template(cadet_test_recipe_path)

    with open(cadet_test_recipe_path) as f:
        template = yaml.safe_load(f)

    assert (
        template["source"]["config"]["aws_connection"]
        == yaml_data["source"]["config"]["aws_connection"]
    )
    assert (
        template["source"]["config"]["manifest_path"]
        == yaml_data["source"]["config"]["manifest_path"]
    )
    assert (
        template["source"]["config"]["catalog_path"]
        == yaml_data["source"]["config"]["catalog_path"]
    )

    assert template["source"]["config"]["run_results_paths"] == [
        "s3://mojap-derived-tables/prod/run_artefacts/run_results.json",
        "s3://mojap-derived-tables/prod/run_artefacts/123/run_results.json",
    ]

    os.remove(cadet_test_recipe_path)


@patch("boto3.client")
@patch("ingestion.cadet_run_results.datetime")
def test_get_run_result_paths(mock_datetime, mock_boto3_client):
    mock_s3_client = mock_boto3_client.return_value
    mock_paginator = mock_s3_client.get_paginator.return_value
    mock_response_iterator = mock_paginator.paginate.return_value
    mock_datetime.now.return_value = datetime(2023, 10, 10, tzinfo=timezone.utc)
    mock_datetime.timedelta = timedelta

    mock_response_iterator.__iter__.return_value = [
        {
            "Contents": [
                {
                    "Key": "prod/run_artefacts/run_results.json",
                    "LastModified": datetime(2023, 10, 9, tzinfo=timezone.utc),
                },
                {
                    "Key": "prod/run_artefacts/123/run_results.json",
                    "LastModified": datetime(2023, 10, 9, tzinfo=timezone.utc),
                },
                {
                    "Key": "prod/run_artefacts/123/run_results.json",
                    "LastModified": datetime(2023, 9, 8, tzinfo=timezone.utc),
                },
                {
                    "Key": "prod/run_artefacts/other_file.json",
                    "LastModified": datetime(2023, 10, 9, tzinfo=timezone.utc),
                },
            ]
        }
    ]

    result = get_cadet_run_result_paths(days=1)
    assert result == [
        "s3://mojap-derived-tables/prod/run_artefacts/run_results.json",
        "s3://mojap-derived-tables/prod/run_artefacts/123/run_results.json",
    ]
