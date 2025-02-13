import copy
import json

import boto3
import pytest

from ingestion.post_ingestion_checks import (
    _remove_empty_dicts,
    check_is_part_of_relationships,
    compare_environment_counts,
)

test_parsed_query_result = {
    "dbt": {
        "domains": [
            {"value": "urn:li:domain:Probation", "count": 3544},
            {"value": "urn:li:domain:Prison", "count": 1539},
            {"value": "urn:li:domain:Courts", "count": 953},
            {"value": "urn:li:domain:Risk", "count": 588},
            {"value": "urn:li:domain:Staging", "count": 149},
            {"value": "urn:li:domain:People", "count": 141},
            {"value": "urn:li:domain:Civil", "count": 135},
            {"value": "urn:li:domain:General", "count": 131},
            {"value": "urn:li:domain:OPG", "count": 110},
            {"value": "urn:li:domain:Electronic monitoring", "count": 86},
            {"value": "urn:li:domain:Bold", "count": 38},
            {"value": "urn:li:domain:Property", "count": 22},
            {"value": "urn:li:domain:Interventions", "count": 17},
            {"value": "urn:li:domain:Finance", "count": 16},
            {"value": "urn:li:domain:Criminal history", "count": 14},
            {"value": "urn:li:domain:Development sandpit", "count": 14},
            {"value": "urn:li:domain:Victims case management", "count": 12},
        ],
        "owners": [
            {"value": "urn:li:corpuser:john.doe", "count": 313},
            {"value": "urn:li:corpuser:jane.smith", "count": 159},
            {"value": "urn:li:corpuser:alice.jones", "count": 143},
            {"value": "urn:li:corpuser:bob.brown", "count": 78},
            {"value": "urn:li:corpuser:charlie.davis", "count": 43},
            {"value": "urn:li:corpuser:eve.martin", "count": 29},
        ],
        "_entityType": [
            {"value": "DATASET", "count": 13340},
            {"value": "CONTAINER", "count": 127},
            {"value": "CHART", "count": 0},
            {"value": "CORP_USER", "count": 0},
            {"value": "SCHEMA_FIELD", "count": 0},
            {"value": "BUSINESS_ATTRIBUTE", "count": 0},
            {"value": "DOMAIN", "count": 0},
            {"value": "DATA_JOB", "count": 0},
            {"value": "GLOSSARY_TERM", "count": 0},
            {"value": "DATA_FLOW", "count": 0},
            {"value": "TAG", "count": 0},
            {"value": "DATA_PRODUCT", "count": 0},
            {"value": "CORP_GROUP", "count": 0},
            {"value": "DASHBOARD", "count": 0},
            {"value": "GLOSSARY_NODE", "count": 0},
        ],
        "tags": [
            {"value": "urn:li:tag:dc_cadet", "count": 9452},
            {"value": "urn:li:tag:retry", "count": 2716},
            {"value": "urn:li:tag:nomis_daily", "count": 1483},
            {"value": "urn:li:tag:curated", "count": 1428},
            {"value": "urn:li:tag:dc_display_in_catalogue", "count": 1389},
            {"value": "urn:li:tag:daily", "count": 672},
            {"value": "urn:li:tag:xhibit", "count": 641},
            {"value": "urn:li:tag:paused", "count": 234},
            {"value": "urn:li:tag:monthly", "count": 149},
            {"value": "urn:li:tag:weekly", "count": 139},
            {"value": "urn:li:tag:caseman_curated", "count": 133},
            {"value": "urn:li:tag:avature_hourly", "count": 95},
            {"value": "urn:li:tag:ems", "count": 77},
            {"value": "urn:li:tag:prison-population", "count": 66},
            {"value": "urn:li:tag:ems_cap_dw", "count": 61},
            {"value": "urn:li:tag:prison-population2", "count": 43},
            {"value": "urn:li:tag:CR_PIPELINE", "count": 38},
            {"value": "urn:li:tag:opg_daily", "count": 24},
            {"value": "urn:li:tag:static", "count": 24},
            {"value": "urn:li:tag:ems_am", "count": 16},
            {"value": "urn:li:tag:exclude", "count": 16},
            {"value": "urn:li:tag:UPW_PIPELINE", "count": 11},
            {"value": "urn:li:tag:CONTACT_PIPELINE", "count": 8},
            {"value": "urn:li:tag:bold_daily", "count": 8},
            {"value": "urn:li:tag:cstr-hmpps", "count": 6},
            {"value": "urn:li:tag:bold-aifld-drug-recovery-wings", "count": 4},
            {"value": "urn:li:tag:bold_case_info_dashboard_daily", "count": 2},
            {"value": "urn:li:tag:ems_api_mart_testdata", "count": 1},
            {"value": "urn:li:tag:bold_daily_dev", "count": 1},
        ],
        "platform": [{"value": "urn:li:dataPlatform:dbt", "count": 0}],
        "entity": [
            {"value": "DATASET", "count": 13340},
            {"value": "CONTAINER", "count": 127},
            {"value": "BUSINESS_ATTRIBUTE", "count": 0},
            {"value": "CORP_USER", "count": 0},
            {"value": "CORP_GROUP", "count": 0},
            {"value": "DATA_PRODUCT", "count": 0},
            {"value": "DATA_FLOW", "count": 0},
            {"value": "GLOSSARY_NODE", "count": 0},
            {"value": "DATA_JOB", "count": 0},
            {"value": "DOMAIN", "count": 0},
            {"value": "SCHEMA_FIELD", "count": 0},
            {"value": "TAG", "count": 0},
            {"value": "GLOSSARY_TERM", "count": 0},
            {"value": "CHART", "count": 0},
            {"value": "DASHBOARD", "count": 0},
        ],
    },
    "glue": {
        "domains": [
            {"value": "urn:li:domain:People", "count": 38},
            {"value": "urn:li:domain:Courts", "count": 32},
            {"value": "urn:li:domain:General", "count": 8},
        ],
        "owners": [
            {"value": "urn:li:corpuser:alex.johnson", "count": 38},
            {"value": "urn:li:corpuser:samuel.green", "count": 23},
            {"value": "urn:li:corpuser:emma.wilson", "count": 9},
            {"value": "urn:li:corpuser:oliver.brown", "count": 8},
        ],
        "_entityType": [
            {"value": "DATA_FLOW", "count": 116},
            {"value": "DATASET", "count": 88},
            {"value": "CONTAINER", "count": 10},
            {"value": "CHART", "count": 0},
            {"value": "CORP_USER", "count": 0},
            {"value": "SCHEMA_FIELD", "count": 0},
            {"value": "BUSINESS_ATTRIBUTE", "count": 0},
            {"value": "DOMAIN", "count": 0},
            {"value": "DATA_JOB", "count": 0},
            {"value": "GLOSSARY_TERM", "count": 0},
            {"value": "TAG", "count": 0},
            {"value": "DATA_PRODUCT", "count": 0},
            {"value": "CORP_GROUP", "count": 0},
            {"value": "DASHBOARD", "count": 0},
            {"value": "GLOSSARY_NODE", "count": 0},
        ],
        "tags": [{"value": "urn:li:tag:dc_display_in_catalogue", "count": 97}],
        "platform": [{"value": "urn:li:dataPlatform:glue", "count": 0}],
        "entity": [
            {"value": "DATA_FLOW", "count": 116},
            {"value": "DATASET", "count": 88},
            {"value": "CONTAINER", "count": 10},
            {"value": "BUSINESS_ATTRIBUTE", "count": 0},
            {"value": "CORP_USER", "count": 0},
            {"value": "CORP_GROUP", "count": 0},
            {"value": "DATA_PRODUCT", "count": 0},
            {"value": "GLOSSARY_NODE", "count": 0},
            {"value": "DATA_JOB", "count": 0},
            {"value": "DOMAIN", "count": 0},
            {"value": "SCHEMA_FIELD", "count": 0},
            {"value": "TAG", "count": 0},
            {"value": "GLOSSARY_TERM", "count": 0},
            {"value": "CHART", "count": 0},
            {"value": "DASHBOARD", "count": 0},
        ],
    },
}


def test_compare_environment_counts():
    prod_results = copy.deepcopy(test_parsed_query_result)
    # pop some values to simulate missing values
    test_parsed_query_result["dbt"]["domains"].pop(0)
    test_parsed_query_result["dbt"]["tags"].pop(3)
    test_parsed_query_result["glue"]["owners"][0] = {
        "value": "urn:li:corpuser:alex.johnson",
        "count": 19,
    }
    comparison_results = compare_environment_counts(
        platforms=["dbt", "glue"],
        prod_results=prod_results,
        preprod_results=test_parsed_query_result,
    )

    assert comparison_results["missing_values"] == {
        "dbt": {
            "domains": {
                "missing_in_preprod": [
                    "urn:li:domain:Probation",
                ]
            },
            "tags": {
                "missing_in_preprod": ["urn:li:tag:curated"],
            },
        }
    }

    assert comparison_results["mismatched_counts"] == {
        "glue": {
            "owners": {"urn:li:corpuser:alex.johnson": 0.5},
        },
    }


def test_get_table_database_mappings(table_database_mappings):
    assert table_database_mappings == {
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.prison_database.table2,PROD)": "urn:li:container:b17e173b8950dee2415a3119fb7c9d12",
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.probation_database.table1,PROD)": "urn:li:container:ea9744b8004d93b716687bab12438c90",
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.courts_data.table1,PROD)": "urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e",
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.hq_database.table1,PROD)": "urn:li:container:48e5e41ce461da41f0333b67a322fb99",
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.ref_database.postcodes,PROD)": "urn:li:container:27c5c4df57bf429bf9e56e51b30003ed",
    }


@pytest.mark.parametrize(
    "table_database_mappings, expected_result",
    [
        (False, []),
        (True, ["urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.no_relations)"]),
    ],
    indirect=["table_database_mappings"],
)
def test_check_is_part_of_relationships(
    table_database_mappings, expected_result, mock_datahub_graph
):
    missing_relations = check_is_part_of_relationships(
        table_database_mappings, mock_datahub_graph
    )
    assert missing_relations == expected_result


@pytest.mark.parametrize(
    "in_dict, out_dict",
    [
        (None, None),
        ({}, {}),
        ({"a": {}}, {}),
        ({"a": {"b": {}}}, {}),
        ({"a": {"b": {"c": {}}}}, {}),
        ({"a": {"b": {"c": {"d": {}}}}, "e": {"f": 1}}, {"e": {"f": 1}}),
    ],
)
def test_remove_empty_dicts(in_dict, out_dict):
    cleaned_dict = _remove_empty_dicts(in_dict)
    assert cleaned_dict == out_dict
