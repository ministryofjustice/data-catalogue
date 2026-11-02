from types import SimpleNamespace

from datahub.ingestion.source.dbt.dbt_common import DBTProject
from datahub.ingestion.source.dbt.dbt_core import DBTCoreSource

from ingestion.cadet_dbt_source.source import CadetDBTSource


def make_project(nodes):
    return DBTProject(
        nodes=nodes,
        exposures=[],
        metrics=None,
        platform_instance=None,
        project_name="project",
        manifest_path="manifest.json",
        artifact_props={},
        catalog_generated_at=None,
        manifest_generated_at=None,
    )


def test_load_projects_removes_display_tag_for_excluded_nodes(monkeypatch):
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

    def fake_load_projects(self):
        yield make_project([excluded_node, included_node])

    monkeypatch.setattr(DBTCoreSource, "load_projects", fake_load_projects)

    source = CadetDBTSource.__new__(CadetDBTSource)
    source.config = SimpleNamespace(tag_prefix="")

    (project,) = CadetDBTSource.load_projects(source)

    assert project.nodes[0].tags == ["business_critical"]
    assert project.nodes[1].tags == ["dc_display_in_catalogue", "business_critical"]


def test_load_projects_respects_tag_prefix(monkeypatch):
    prefixed_node = SimpleNamespace(
        dbt_name="model.project.staging_fms__stg_example",
        database="awsdatacatalog",
        schema="staging_fms",
        name="staging_fms__stg_example",
        alias=None,
        tags=["dbt:dc_display_in_catalogue", "dbt:team_alpha"],
    )

    def fake_load_projects(self):
        yield make_project([prefixed_node])

    monkeypatch.setattr(DBTCoreSource, "load_projects", fake_load_projects)

    source = CadetDBTSource.__new__(CadetDBTSource)
    source.config = SimpleNamespace(tag_prefix="dbt:")

    (project,) = CadetDBTSource.load_projects(source)

    assert project.nodes[0].tags == ["dbt:team_alpha"]
