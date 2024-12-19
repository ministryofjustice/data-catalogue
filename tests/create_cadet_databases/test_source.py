from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)

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

    # Parse the result into a nested structure, indexed first by URN, then by aspect
    # TODO: maybe this should be further grouped by entity type
    metadata_by_urn = {}
    for wu in source.get_workunits():
        if urn := wu.get_urn():
            aspects_by_name = metadata_by_urn.setdefault(urn, {})
            aspects_by_name.setdefault(wu.metadata.aspectName, []).append(
                wu.metadata.aspect
            )

    return metadata_by_urn


def extract_tag_names(global_tags_list):
    return [tag.tag for association in global_tags_list for tag in association.tags]


def test_tags(mock_datahub_graph):
    metadata = run_source(mock_datahub_graph)

    courts_data_tags = extract_tag_names(
        metadata["urn:li:container:1e7a7a180ed4f1215bff62f4ce93993e"]["globalTags"]
    )
    probation_database_tags = extract_tag_names(
        metadata["urn:li:container:ea9744b8004d93b716687bab12438c90"]["globalTags"]
    )
    prison_database_tags = extract_tag_names(
        metadata["urn:li:container:b17e173b8950dee2415a3119fb7c9d12"]["globalTags"]
    )

    ref_database_tags = extract_tag_names(
        metadata["urn:li:container:27c5c4df57bf429bf9e56e51b30003ed"]["globalTags"]
    )
    hq_database_tags = extract_tag_names(
        metadata["urn:li:container:48e5e41ce461da41f0333b67a322fb99"]["globalTags"]
    )

    assert set(courts_data_tags) == {
        "urn:li:tag:Courts",
        "urn:li:tag:dc_display_in_catalogue",
    }
    assert set(probation_database_tags) == {
        "urn:li:tag:Probation",
        "urn:li:tag:dc_display_in_catalogue",
    }
    assert set(prison_database_tags) == {
        "urn:li:tag:Prison",
        "urn:li:tag:dc_display_in_catalogue",
    }
    assert set(ref_database_tags) == {
        "urn:li:tag:General",
        "urn:li:tag:dc_display_in_catalogue",
    }
    assert set(hq_database_tags) == {
        "urn:li:tag:HQ",
        "urn:li:tag:dc_display_in_catalogue",
    }
