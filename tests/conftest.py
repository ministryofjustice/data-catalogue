import time
from typing import Dict, Optional, Type
from unittest.mock import MagicMock, create_autospec
import boto3
import pytest
import types
from avrogen.dict_wrapper import DictWrapper
from datahub.ingestion.graph.client import DataHubGraph
from moto import mock_s3
import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper


@pytest.fixture
def mock_datahub_graph():
    class MockDataHubGraphContext:
        pipeline_name: str = "test_pipeline"
        run_id: str = "test_run"

        def __init__(self) -> None:
            """
            Create a new monkey-patched instance of the DataHubGraph graph client.
            """
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

        def monkey_patch_emit_mcp(
            self, mcpw: MetadataChangeProposalWrapper
        ) -> None:
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

    mock_datahub_graph_ctx = MockDataHubGraphContext()
    return mock_datahub_graph_ctx.mock_graph


@pytest.fixture
def mock_time(monkeypatch):
    def fake_time():
        return 1615443388.0975091

    monkeypatch.setattr(time, "time", fake_time)

    yield

@pytest.fixture(autouse=True)
def mock_manifest_in_s3():
    with mock_s3():
        s3 = boto3.client("s3")
        bucket = "mojap-derived-tables"
        key = 'prod/run_artefacts/latest/target/manifest.json'
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'},
        )
        with open(
            os.path.join(os.path.dirname(__file__), "data", "manifest.json"),
            "rb",
        ) as body:
            s3.put_object(
                Body=body,
                Bucket=bucket,
                Key=key,
            )

        yield
