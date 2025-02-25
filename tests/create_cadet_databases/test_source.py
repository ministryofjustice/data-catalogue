from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from utils import WorkunitInspector

from ingestion.create_cadet_databases_source.source import (
    CreateCadetDatabases,
    CreateCadetDatabasesConfig,
)


def run_source(mock_datahub_graph):
    source = CreateCadetDatabases(
        config=CreateCadetDatabasesConfig(
            manifest_s3_uri="s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
            database_metadata_s3_uri="s3://test_bucket/prod/run_artefacts/latest/target/database_metadata.json",
            stateful_ingestion=StatefulStaleMetadataRemovalConfig(
                remove_stale_metadata=True
            ),
        ),
        ctx=PipelineContext(run_id="abc", graph=mock_datahub_graph),
    )

    return WorkunitInspector(source.get_workunits())


def test_tags(mock_datahub_graph):
    metadata = run_source(mock_datahub_graph)

    courts_data_tags = metadata.entity(
        "urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e"
    ).tag_names

    probation_database_tags = metadata.entity(
        "urn:li:container:ea9744b8004d93b716687bab12438c90"
    ).tag_names

    prison_database_tags = metadata.entity(
        "urn:li:container:b17e173b8950dee2415a3119fb7c9d12"
    ).tag_names

    ref_database_tags = metadata.entity(
        "urn:li:container:27c5c4df57bf429bf9e56e51b30003ed"
    ).tag_names

    hq_database_tags = metadata.entity(
        "urn:li:container:48e5e41ce461da41f0333b67a322fb99"
    ).tag_names

    assert set(courts_data_tags) == {
        "urn:li:tag:Courts and tribunals",
        "urn:li:tag:dc_display_in_catalogue",
    }
    assert set(probation_database_tags) == {
        "urn:li:tag:Prisons and probation",
        "urn:li:tag:dc_display_in_catalogue",
    }
    assert set(prison_database_tags) == {
        "urn:li:tag:Prisons and probation",
        "urn:li:tag:dc_display_in_catalogue",
        "urn:li:tag:prison",
        "urn:li:tag:secure",
    }
    assert set(ref_database_tags) == {
        "urn:li:tag:dc_display_in_catalogue",
    }
    assert set(hq_database_tags) == {
        "urn:li:tag:dc_display_in_catalogue",
    }
