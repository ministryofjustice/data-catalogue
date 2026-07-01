from types import SimpleNamespace

from datahub.ingestion.source.dbt.dbt_core import DBTCoreSource

from ingestion.cadet_dbt_source.source import CadetDBTSource


def test_load_manifest_and_catalog_removes_display_tag_for_excluded_nodes(monkeypatch):
    excluded_node = SimpleNamespace(
        dbt_name="model.project.staging_fms__stg_alm_asset_fms",
        database="awsdatacatalog",
        schema="staging_fms",
        name="staging_fms__stg_alm_asset_fms",
        alias=None,
        tags=["dc_display_in_catalogue", "business_critical"],
    )
    included_node = SimpleNamespace(
        dbt_name="model.project.curated__case_notes",
        database="awsdatacatalog",
        schema="curated_prison",
        name="curated__case_notes",
        alias=None,
        tags=["dc_display_in_catalogue", "business_critical"],
    )

    def fake_load_manifest_and_catalog(self):
        return ([excluded_node, included_node], "m_schema", "m_ver", "athena", None, None)

    monkeypatch.setattr(DBTCoreSource, "loadManifestAndCatalog", fake_load_manifest_and_catalog)

    source = CadetDBTSource.__new__(CadetDBTSource)
    source.config = SimpleNamespace(tag_prefix="")

    nodes, *_ = CadetDBTSource.loadManifestAndCatalog(source)

    assert nodes[0].tags == ["business_critical"]
    assert nodes[1].tags == ["dc_display_in_catalogue", "business_critical"]


def test_load_manifest_and_catalog_respects_tag_prefix(monkeypatch):
    prefixed_node = SimpleNamespace(
        dbt_name="model.project.staging_fms__stg_example",
        database="awsdatacatalog",
        schema="staging_fms",
        name="staging_fms__stg_example",
        alias=None,
        tags=["dbt:dc_display_in_catalogue", "dbt:team_alpha"],
    )

    def fake_load_manifest_and_catalog(self):
        return ([prefixed_node], "m_schema", "m_ver", "athena", None, None)

    monkeypatch.setattr(DBTCoreSource, "loadManifestAndCatalog", fake_load_manifest_and_catalog)

    source = CadetDBTSource.__new__(CadetDBTSource)
    source.config = SimpleNamespace(tag_prefix="dbt:")

    nodes, *_ = CadetDBTSource.loadManifestAndCatalog(source)

    assert nodes[0].tags == ["dbt:team_alpha"]
