from io import BufferedReader
from typing import Iterable, Optional

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps, Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import ChartSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import BrowsePathsV2Class, ChartInfoClass

from .api_client import JusticeDataAPIClient
from .config import JusticeDataAPIConfig


@platform_name("File")
@config_class(JusticeDataAPIConfig)
@support_status(SupportStatus.CERTIFIED)
class JusticeDataAPISource(TestableSource):
    """
    This plugin pulls metadata from the Justice Data API
    """

    def __init__(self, ctx: PipelineContext, config: JusticeDataAPIConfig):
        self.ctx = ctx
        self.config = config
        self.report = SourceReport()
        self.fp: Optional[BufferedReader] = None
        self.client = JusticeDataAPIClient(config.base_url)
        self.platform_name = "justice-data"
        self.web_url = self.config.base_url.removesuffix("/api").removesuffix("/api/")

    @classmethod
    def create(cls, config_dict, ctx):
        config = JusticeDataAPIConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # TODO generate metadata for the dashboard itself

        for chart_data in self.client.list_all():
            mce = self._make_chart(chart_data)
            wu = MetadataWorkUnit("single_mce", mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def get_report(self):
        return self.report

    def _make_chart(self, chart_data) -> MetadataChangeEvent:
        chart_urn = builder.make_chart_urn(self.platform_name, chart_data["id"])
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[Status(removed=False)],
        )

        title = chart_data["name"]

        # TODO: generate a fully qualified name?
        chart_info = ChartInfoClass(
            description=chart_data.get("description") or "",
            title=title,
            lastModified=ChangeAuditStamps(),  # TODO: add timestamps here
            chartUrl=self.web_url + chart_data.get("permalink", ""),
        )
        chart_snapshot.aspects.append(chart_info)

        # TODO: browse paths requires IDs, not just titles
        breadcrumb = chart_data.get("breadcrumb")
        breadcrumb.append(title)
        # browse_path = BrowsePathsV2Class(path=["/justice-data/" + "/".join(breadcrumb)])
        # chart_snapshot.aspects.append(browse_path)

        # TODO: propagate ownership from dashboard

        chart_mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)

        # TODO: add embed url?

        return chart_mce

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(
                capable=False,
                failure_reason=f"Haven't implemented this yet!",
            )
        )
