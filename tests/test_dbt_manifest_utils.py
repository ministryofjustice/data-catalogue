import pytest
import json

from ingestion.dbt_manifest_utils import (
    format_domain_name,
    parse_database_and_table_names,
)


@pytest.mark.parametrize(
    "original,expected",
    [
        ("foo", "Foo"),
        ("electronic_monitoring", "Electronic monitoring"),
        ("opg", "OPG"),
        ("aBcDe", "Abcde"),
    ],
)
def test_format_domain_name(original, expected):
    assert format_domain_name(original) == expected


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
            "test_derived_tables_dev_dbt",
            "table1",
        ),
    ],
)
def test_parse_database_and_table_names(node, database, table):
    database_name, table_name = parse_database_and_table_names(node)
    assert database_name == database
    assert table_name == table
