from typing import Optional

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from pydantic import Field

# These map the api ids to domains as set by create_cadet_database_source.py
# and all children of these ids will inherit their parent's domain
# format {api_id: domain}
ID_TO_DOMAIN_MAPPING = {
    "prisons": "prison",
    "probation": "probation",
    "courts": "courts",
    "electronic-monitoring": "electronic monitoring",
    "electronic-monitoring-performance": "electronic monitoring",
    "bass": "probation",
    "cjs-crime": "general",
    "cjs-reoffending": "general",
    "cjs-sentence-types": "courts",
    "cjs-entrants": "courts",
}


class JusticeDataAPIConfig(StatefulIngestionConfigBase):
    base_url: str = Field(description="URL to justice data API")
    exclude_id_list: list[str] = Field(
        description="list of ids to exclude from the ingestion, inclusive of that id and all children",
        default=[],
    )
    access_requirements: str = Field(
        description="""
            Paragraph explaning whether there are any specific access requirements related these data.
            Justice data being published can have a blanket para but needs to be different from the
            default section in find-moj-data""",
        default="",
    )
    default_owner_email: str = Field(
        description="""
            The owner email will default to this email if the `ownerEmail key is not found
            at the /publications endpoint""",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        description="""
            Can configure whether the ingestion is be be staeful and can remove stale metadata.
            see https://datahubproject.io/docs/metadata-ingestion/docs/dev_guides/stateful/#stale-entity-removal
            """,
        default=None,
    )
