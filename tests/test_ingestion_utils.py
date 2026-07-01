import json

import pytest

from ingestion.ingestion_utils import (
    get_tags,
    is_excluded_name,
    parse_database_and_table_names,
    should_display_dbt_manifest_node,
)

with open("tests/data/manifest.json") as f:
    test_manifest = json.load(f)


@pytest.mark.parametrize(
    "node, database, table",
    [
        (
            test_manifest["nodes"]["model.test_derived_tables.prison"],
            "prison_database",
            "table1",
        ),
        (
            test_manifest["nodes"]["model.test_derived_tables.hq"],
            "hq_database",
            "table1",
        ),
        (
            test_manifest["nodes"]["source.test_derived_tables.nope"],
            "test_derived_tables",
            "table1",
        ),
        (
            test_manifest["nodes"]["seed.test_derived_tables.nope"],
            "ref_database",
            "postcodes",
        ),
    ],
)
def test_parse_database_and_table_names(node, database, table):
    database_name, table_name = parse_database_and_table_names(node)
    assert database_name == database
    assert table_name == table


@pytest.mark.parametrize(
    "name, expected",
    [
        ("stg_orders", True),
        ("staging_fms", True),
        ("testing_table", True),
        ("production_model", False),
        (None, False),
    ],
)
def test_is_excluded_name(name, expected):
    assert is_excluded_name(name) is expected


def test_should_display_dbt_manifest_node_false_when_excluded_keyword_present():
    node = {
        "unique_id": "model.project.curated__orders",
        "database": "awsdatacatalog",
        "schema": "staging_fms",
        "name": "curated__orders",
        "alias": None,
        "identifier": None,
        "fqn": ["project", "domain", "staging_fms", "curated__orders"],
    }

    assert should_display_dbt_manifest_node(node) is False


def test_get_tags_omits_display_tag_for_excluded_node():
    node = {
        "tags": ["dc_display_in_catalogue"],
        "resource_type": "model",
        "unique_id": "model.project.staging_fms__stg_alm_asset_fms",
        "database": "awsdatacatalog",
        "schema": "staging_fms",
        "name": "staging_fms__stg_alm_asset_fms",
        "alias": None,
        "identifier": None,
        "fqn": [
            "project",
            "domain",
            "staging_fms",
            "staging_fms__stg_alm_asset_fms",
        ],
    }

    assert get_tags(node) == set()


def test_get_tags_adds_display_tag_for_displayable_seed():
    node = {
        "tags": [],
        "resource_type": "seed",
        "unique_id": "seed.project.reference__postcodes",
        "database": "awsdatacatalog",
        "schema": "reference_data",
        "name": "reference__postcodes",
        "alias": None,
        "identifier": None,
        "fqn": ["project", "domain", "reference_data", "reference__postcodes"],
    }

    assert get_tags(node) == {"dc_display_in_catalogue"}
