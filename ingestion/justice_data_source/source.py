import logging
from datetime import datetime
from io import BufferedReader
from typing import Any, Iterable, Literal, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    DashboardInfoClass,
    GlobalTagsClass,
    OwnershipClass,
    TagAssociationClass,
)
from datahub.utilities.time import datetime_to_ts_millis

from ingestion.ingestion_utils import get_subject_areas
from ingestion.utils import report_generator_time

from .api_client import JusticeDataAPIClient
from .config import JusticeDataAPIConfig

logging.basicConfig(level=logging.DEBUG)


@platform_name("File")
@config_class(JusticeDataAPIConfig)
@support_status(SupportStatus.CERTIFIED)
class JusticeDataAPISource(StatefulIngestionSourceBase):
    """
    This plugin pulls metadata from the Justice Data API
    """

    def __init__(
        self,
        ctx: PipelineContext,
        config: JusticeDataAPIConfig,
        validate_subject_areas: bool = True,
    ):
        super().__init__(config, ctx)

        self.ctx = ctx
        self.config = config
        self.report = StaleEntityRemovalSourceReport()
        self.fp: Optional[BufferedReader] = None
        self.client = JusticeDataAPIClient(config.base_url, config.default_owner_email)
        if validate_subject_areas:
            self.client.validate_subject_areas(get_subject_areas())
        self.platform_name = "justice-data"
        self.web_url = self.config.base_url.removesuffix("/api").removesuffix("/api/")

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, ctx
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = JusticeDataAPIConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self):
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    @report_generator_time
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        all_chart_data = self.client.list_all(self.config.exclude_id_list)

        # creates each chart entity
        for chart_data in all_chart_data:
            for wu in self._make_chart(chart_data):
                self.report.report_workunit(wu)
                yield wu

        # make the dashboard itself
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
            description="A public facing service containing data visualisations across multiple MoJ subject areas. Official statistical publications are the source data for everything contained within this dashboard.",
            lastModified=ChangeAuditStamps(),  # TODO: add timestamps here
            externalUrl="https://data.justice.gov.uk/",
            charts=chart_urns,
            customProperties={
                "dc_access_requirements": self.config.access_requirements
            },
        )
        dashboard_snapshot.aspects.append(dashboard_info)

        # add tag so entity displays in find-moj-data
        display_tag = self._make_tags_aspect()
        dashboard_snapshot.aspects.append(display_tag)

        dashboard_mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
        return dashboard_mce

    def _make_chart(
        self, chart_data: dict[str, Any]
    ):  # -> Generator[MetadataWorkUnit, Any, None]:
        chart_urn = builder.make_chart_urn(self.platform_name, chart_data["id"])

        title = chart_data["name"]
        refresh_period = self._format_update_frequency(chart_data.get("refresh_period"))
        publication_date = chart_data.get("last_updated_timestamp")

        # TODO: generate a fully qualified name?
        chart_info = ChartInfoClass(
            description=chart_data.get("description") or "",
            title=title,
            lastModified=ChangeAuditStamps(
                lastModified=self._format_audit_stamp(publication_date)
            ),
            chartUrl=self.web_url + chart_data.get("permalink", ""),
            lastRefreshed=(
                datetime_to_ts_millis(publication_date) if publication_date else None
            ),
            customProperties={
                "refresh_period": refresh_period or "",
                "dc_access_requirements": self.config.access_requirements,
                "security_classification": "Official - For public release",
                "dc_team_email": chart_data["owner_email"],
            },
        )

        tag_aspect = self._make_tags_aspect(chart_data["subject_areas"])

        # wipe all owners (this can be removed if/when we reintroduce owners to Justice Data charts)
        owners = OwnershipClass(owners=[])

        yield from [
            mcp.as_workunit()
            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=chart_urn,
                aspects=[chart_info, tag_aspect, Status(removed=False), owners],
            )
        ]

    def _make_tags_aspect(self, tag_names: list[str] = []) -> GlobalTagsClass:
        if "dc_display_in_catalogue" not in tag_names:
            tag_names.append("dc_display_in_catalogue")
        tag_urns = [builder.make_tag_urn(tag=tag_name) for tag_name in tag_names]
        tag_assocations = [TagAssociationClass(tag_urn) for tag_urn in tag_urns]
        tags = GlobalTagsClass(tags=tag_assocations)
        return tags

    def _format_audit_stamp(self, maybe_date: datetime | None) -> AuditStamp | None:
        if not maybe_date:
            return None

        return AuditStamp(
            time=datetime_to_ts_millis(maybe_date),
            actor=mce_builder.make_user_urn("unknown"),
        )

    def _format_update_frequency(
        self, maybe_frequency: str | None
    ) -> Literal["Annual"] | Literal["Quarterly"] | Literal["AdHoc"] | Literal[""]:
        if maybe_frequency is not None and maybe_frequency not in (
            "Annual",
            "Quarterly",
            "AdHoc",
        ):
            raise ValueError(maybe_frequency)

        # Datahub custom properties cannot be None, so default to
        # empty string instead.
        return maybe_frequency or ""

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(
                capable=False,
                failure_reason=f"Haven't implemented this yet!",
            )
        )
