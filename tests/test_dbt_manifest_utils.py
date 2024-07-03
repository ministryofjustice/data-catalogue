import pytest

from ingestion.dbt_manifest_utils import format_domain_name


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
