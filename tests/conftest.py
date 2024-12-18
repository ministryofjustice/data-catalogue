import json
import os
import sys
import time
import types
from typing import Dict, List, Optional
from unittest.mock import create_autospec

import boto3
import pytest
import yaml
from avrogen.dict_wrapper import DictWrapper
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, RelatedEntity
from moto import mock_s3

sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))

from ingestion.post_ingestion_checks import _get_table_database_mappings


@pytest.fixture
def publication_mappings():
    with open(
        os.path.join(
            os.path.dirname(__file__), "data", "test_publication_mappings.yml"
        ),
        "r",
    ) as file:
        return yaml.safe_load(file)


@pytest.fixture
def default_owner_email() -> str:
    return "not.me@justice.gov.uk"


@pytest.fixture
def default_contact_email() -> str:
    return "the.contact@justice.gov.uk"


@pytest.fixture
def manifest():
    s3_client = boto3.client("s3")
    response = s3_client.get_object(
        Bucket="test_bucket", Key="prod/run_artefacts/latest/target/manifest.json"
    )
    content = response["Body"].read().decode("utf-8")
    manifest = json.loads(content, strict=False)
    return manifest


@pytest.fixture
def table_database_mappings(manifest, request):
    param = getattr(request, "param", False)
    mappings = _get_table_database_mappings(manifest)
    if param:
        mappings["urn:li:dataset:(urn:li:dataPlatform:dbt,cadet.no_relations)"] = (
            "urn:li:container:27c5c4df57bf429bf9e56e51b30003ed"
        )
    return mappings


@pytest.fixture
def mock_datahub_graph(manifest):
    class MockDataHubGraphContext:
        pipeline_name: str = "test_pipeline"
        run_id: str = "test_run"

        def __init__(self, manifest) -> None:
            """
            Create a new monkey-patched instance of the DataHubGraph graph client.
            """
            self.table_database_mappings = _get_table_database_mappings(manifest)
            # ensure this mock keeps the same api of the original class
            self.mock_graph = create_autospec(DataHubGraph)
            # Make server stateful ingestion capable
            self.mock_graph.get_config.return_value = {"statefulIngestionCapable": True}
            # Bind mock_graph's emit_mcp to testcase's monkey_patch_emit_mcp so that we can emulate emits.
            self.mock_graph.emit_mcp = types.MethodType(
                self.monkey_patch_emit_mcp, self.mock_graph
            )
            # Bind mock_graph's get_latest_timeseries_value to monkey_patch_get_latest_timeseries_value
            self.mock_graph.get_latest_timeseries_value = types.MethodType(
                self.monkey_patch_get_latest_timeseries_value, self.mock_graph
            )
            # Tracking for emitted mcps.
            self.mcps_emitted: Dict[str, MetadataChangeProposalWrapper] = {}

            # Bind mock_graph's get_related_entities to monkey_patch_get_related_entities
            self.mock_graph.get_related_entities = types.MethodType(
                self.monkey_patch_get_related_entities, self.mock_graph
            )

        def monkey_patch_emit_mcp(self, mcpw: MetadataChangeProposalWrapper) -> None:
            """
            Mockey patched implementation of DatahubGraph.emit_mcp that caches the mcp locally in memory.
            """
            # Cache the mcpw against the entityUrn
            assert mcpw.entityUrn is not None
            self.mcps_emitted[mcpw.entityUrn] = mcpw

        def monkey_patch_get_latest_timeseries_value(
            self,
            entity_urn: str,
        ) -> Optional[DictWrapper]:
            """
            Monkey patched implementation of DatahubGraph.get_latest_timeseries_value that returns the latest cached aspect
            for a given entity urn.
            """
            # Retrieve the cached mcpw and return its aspect value.
            mcpw = self.mcps_emitted.get(entity_urn)
            if mcpw:
                return mcpw.aspect
            return None

        def monkey_patch_get_related_entities(
            self,
            cls,
            entity_urn: str,
            relationship_types: List[str],
            direction: DataHubGraph.RelationshipDirection,
        ) -> List[RelatedEntity]:
            related_entities = []
            if self.table_database_mappings.get(entity_urn):
                related_entities.append(
                    RelatedEntity(
                        urn=self.table_database_mappings[entity_urn],
                        relationship_type=relationship_types[0],
                    )
                )
            return related_entities

    mock_datahub_graph_ctx = MockDataHubGraphContext(manifest)
    return mock_datahub_graph_ctx.mock_graph


@pytest.fixture
def mock_time(monkeypatch):
    def fake_time():
        return 1615443388.0975091

    monkeypatch.setattr(time, "time", fake_time)

    yield


@pytest.fixture(autouse=True)
def mock_metadata_in_s3():
    """
    mocks both the manifest and database_metadata json files in s3
    """
    with mock_s3():
        s3 = boto3.client("s3")
        bucket = "test_bucket"
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        files = ["manifest.json", "database_metadata.json"]
        for file in files:
            key = f"prod/run_artefacts/latest/target/{file}"

            with open(
                os.path.join(os.path.dirname(__file__), "data", file),
                "rb",
            ) as body:
                s3.put_object(
                    Body=body,
                    Bucket=bucket,
                    Key=key,
                )

        yield
