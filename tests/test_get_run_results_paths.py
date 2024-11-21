from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from ingestion.cadet_run_results import (
    get_cadet_run_result_paths,
    inject_run_result_paths_into_yaml_template,
)


def test_inject_run_results_into_yaml_template(
    mock_get_run_result_paths, mock_yaml_dump, mock_yaml_safe_load, mock_open
):
    pass


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
