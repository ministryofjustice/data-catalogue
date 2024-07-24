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
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    ChartInfoClass,
    GlobalTagsClass,
    TagAssociationClass,
    DomainsClass,
    ChangeTypeClass,
    DashboardInfoClass,
)


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

        all_chart_data = self.client.list_all()
        for chart_data in all_chart_data:
            mce = self._make_chart(chart_data)
            wu = MetadataWorkUnit("single_mce", mce=mce)
            self.report.report_workunit(wu)
            yield wu

        # adds domains to each chart created previously
        for chart_data in all_chart_data:
            if chart_data.get("domain"):
                mcp = self._assign_chart_to_domain(chart_data)
                wu = MetadataWorkUnit("single_mcp", mcp=mcp)
                self.report.report_workunit(wu)
                yield wu

        # TODO generate metadata for the dashboard itself
        chart_urns = [
            builder.make_chart_urn(self.platform_name, chart_data["id"])
            for chart_data in all_chart_data
        ]
        mce = self._make_dashboard(chart_urns)
        wu = MetadataWorkUnit("single_mce", mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def get_report(self):
        return self.report

    def _make_dashboard(self, chart_urns):
        dashboard_urn = builder.make_dashboard_urn(self.platform_name, "Justice Data")
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[Status(removed=False)],
        )

        dashboard_info = DashboardInfoClass(
            title="Justice Data",
            description="A published collection of data visualisations relating to multiple domains",
            lastModified=ChangeAuditStamps(),  # TODO: add timestamps here
            externalUrl="https://data.justice.gov.uk/",
            charts=chart_urns,
        )
        dashboard_snapshot.aspects.append(dashboard_info)

        # add tag so entity displays in find-moj-data
        display_tag = self._make_tags_aspect()
        dashboard_snapshot.aspects.append(display_tag)

        dashboard_mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
        return dashboard_mce

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
            lastRefreshed=(
                int(chart_data["last_updated_timestamp"])
                if chart_data.get("last_updated_timestamp")
                else None
            ),
            customProperties={
                "refresh_frequency": chart_data.get("refresh_frequency", "")
            },
        )
        chart_snapshot.aspects.append(chart_info)

        # add tag so entity displays in find-moj-data
        display_tag = self._make_tags_aspect()
        chart_snapshot.aspects.append(display_tag)

        # TODO: browse paths requires IDs, not just titles
        breadcrumb = chart_data.get("breadcrumb")
        breadcrumb.append(title)
        # browse_path = BrowsePathsV2Class(path=["/justice-data/" + "/".join(breadcrumb)])
        # chart_snapshot.aspects.append(browse_path)

        # TODO: propagate ownership from dashboard

        chart_mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)

        # TODO: add embed url?

        return chart_mce

    def _assign_chart_to_domain(self, chart_data) -> MetadataChangeProposalWrapper:
        """
        because domain cannot be added via a MetadataChangeEvent we need to use
        MetadataChangeProposal
        """
        domain_urn = builder.make_domain_urn(domain=chart_data["domain"])
        chart_urn = builder.make_chart_urn(self.platform_name, chart_data["id"])

        mcp = MetadataChangeProposalWrapper(
            entityType="chart",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=chart_urn,
            aspect=DomainsClass(domains=[domain_urn]),
        )
        return mcp

    def _make_tags_aspect(
        self, tag_names: list[str] = ["dc_display_in_catalogue"]
    ) -> GlobalTagsClass:
        tag_urns = [builder.make_tag_urn(tag=tag_name) for tag_name in tag_names]
        tag_assocations = [TagAssociationClass(tag_urn) for tag_urn in tag_urns]
        tags = GlobalTagsClass(tags=tag_assocations)
        return tags

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(
                capable=False,
                failure_reason=f"Haven't implemented this yet!",
            )
        )
