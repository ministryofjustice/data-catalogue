import os
from datetime import datetime, timedelta, timezone
from unittest.mock import mock_open, patch

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
            "platform_instance": "cadet",
            "target_platform": "athena",
            "target_platform_instance": "athena_cadet",
            "infer_dbt_schemas": True,
            "write_semantics": "OVERRIDE",
            "node_name_pattern": {
                "deny": [
                    ".*use_of_force\\.summary_status_complete_dim.*",
                    ".*use_of_force\\.summary_status_in_progress_dim.*",
                    ".*use_of_force\\.summary_status_submitted_dim.*",
                    ".*data_eng_uploader_prod_calc_release_dates\\.survey_data.*",
                    "source.mojap_derived_tables.oasys.*",
                    "source.mojap_derived_tables.delius.*",
                    ".*avature_stg\\.stg_people_fields_pivoted_wide.*",
                    ".*avature_stg\\.stg_job_fields_pivoted_wide.*",
                    "source.mojap_derived_tables.data_eng_uploader_prod_calc_release_dates.survey_data",
                ]
            },
            "entities_enabled": {
                "test_results": "YES",
                "seeds": "YES",
                "snapshots": "NO",
                "models": "YES",
                "sources": "YES",
                "test_definitions": "YES",
            },
            "stateful_ingestion": {"enabled": True, "remove_stale_metadata": True},
            "include_compiled_code": False,
            "include_column_lineage": False,
            "strip_user_ids_from_email": False,
            "tag_prefix": "",
            "meta_mapping": {
                "dc_data_custodian": {
                    "match": ".*",
                    "operation": "add_owner",
                    "config": {"owner_type": "user", "owner_category": "DATAOWNER"},
                }
            },
        },
    },
    "transformers": [
        {
            "type": "ingestion.transformers.assign_cadet_databases.AssignCadetDatabases",
            "config": {
                "manifest_s3_uri": "s3://mojap-derived-tables/prod/run_artefacts/latest/target/manifest.json"
            },
        },
        {
            "type": "simple_add_dataset_properties",
            "config": {"properties": {"audience": "Internal"}},
        },
    ],
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

    yaml_data["source"]["config"]["run_results_paths"] = [
        "s3://mojap-derived-tables/prod/run_artefacts/run_results.json",
        "s3://mojap-derived-tables/prod/run_artefacts/123/run_results.json",
    ]
    assert template == yaml_data

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
                {
                    "Key": "prod/run_artefacts/deploy-docs/run_results.json",
                    "LastModified": datetime(2023, 11, 10, tzinfo=timezone.utc),
                },
            ]
        }
    ]

    result = get_cadet_run_result_paths(days=1)
    assert result == [
        "s3://mojap-derived-tables/prod/run_artefacts/run_results.json",
        "s3://mojap-derived-tables/prod/run_artefacts/123/run_results.json",
    ]
