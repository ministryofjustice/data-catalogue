import datahub.emitter.mce_builder as mce_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import DatasetPropertiesClass

from ingestion.config import ENV, INSTANCE, PLATFORM
from ingestion.transformers.add_latest_file_timestamp import AddLatestFileTimestamp


class TestAddLatestFileTimestampTransformer:
    def test_adds_latest_file_timestamp_to_custom_properties(self, monkeypatch):
        dataset_urn = mce_builder.make_dataset_urn_with_platform_instance(
            name="prison_database.table1",
            platform=PLATFORM,
            platform_instance=INSTANCE,
            env=ENV,
        )

        monkeypatch.setattr(
            AddLatestFileTimestamp,
            "_build_latest_timestamp_lookup",
            lambda self, manifest: {dataset_urn: "2026-05-14T10:20:30+00:00"},
        )

        transformer = AddLatestFileTimestamp.create(
            {
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
                "aws_region": "eu-west-1",
            },
            PipelineContext(run_id="test_run"),
        )

        aspect = transformer.transform_aspect(
            entity_urn=dataset_urn,
            aspect_name="datasetProperties",
            aspect=DatasetPropertiesClass(
                name="table1",
                customProperties={"security_classification": "Official-Sensitive"},
            ),
        )

        assert isinstance(aspect, DatasetPropertiesClass)
        assert aspect.customProperties == {
            "security_classification": "Official-Sensitive",
            "latest_file_timestamp": "2026-05-14T10:20:30+00:00",
        }

    def test_returns_aspect_unchanged_for_unknown_dataset(self, monkeypatch):
        monkeypatch.setattr(
            AddLatestFileTimestamp,
            "_build_latest_timestamp_lookup",
            lambda self, manifest: {},
        )

        transformer = AddLatestFileTimestamp.create(
            {
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
                "aws_region": "eu-west-1",
            },
            PipelineContext(run_id="test_run"),
        )

        aspect = transformer.transform_aspect(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.unknown.table1,PROD)",
            aspect_name="datasetProperties",
            aspect=DatasetPropertiesClass(
                name="table1",
                customProperties={"security_classification": "Official-Sensitive"},
            ),
        )

        assert isinstance(aspect, DatasetPropertiesClass)
        assert aspect.customProperties == {
            "security_classification": "Official-Sensitive"
        }

    def test_excludes_staging_tables(self, monkeypatch):
        monkeypatch.setattr(
            AddLatestFileTimestamp,
            "_build_latest_timestamp_lookup",
            lambda self, manifest: {},
        )

        transformer = AddLatestFileTimestamp.create(
            {
                "manifest_s3_uri": "s3://test_bucket/prod/run_artefacts/latest/target/manifest.json",
                "aws_region": "eu-west-1",
            },
            PipelineContext(run_id="test_run"),
        )

        test_cases = [
            "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.db.stg_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.db.staging_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.awsdatacatalog.db.int_table,PROD)",
        ]

        for entity_urn in test_cases:
            aspect = transformer.transform_aspect(
                entity_urn=entity_urn,
                aspect_name="datasetProperties",
                aspect=DatasetPropertiesClass(
                    name="table1",
                    customProperties={"security_classification": "Official-Sensitive"},
                ),
            )

            assert isinstance(aspect, DatasetPropertiesClass)
            assert aspect.customProperties == {
                "security_classification": "Official-Sensitive"
            }
            assert "latest_file_timestamp" not in aspect.customProperties