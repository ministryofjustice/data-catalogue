from typing import Optional

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from pydantic import Field


class CreateCadetDatabasesConfig(StatefulIngestionConfigBase):
    manifest_s3_uri: str = Field(
        description="s3 path to dbt manifest json", default=None
    )
    database_metadata_s3_uri: str = Field(
        description="s3 path to database_metadata json", default=None
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        description="""
            Can configure whether the ingestion is be be staeful and can remove stale metadata.
            see https://datahubproject.io/docs/metadata-ingestion/docs/dev_guides/stateful/#stale-entity-removal
            """,
        default=None,
    )
