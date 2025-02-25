import json

import pytest

from ingestion.ingestion_utils import parse_database_and_table_names

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
