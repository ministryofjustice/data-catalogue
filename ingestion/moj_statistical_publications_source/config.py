from typing import Optional

import yaml
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from pydantic import BaseModel, Field

with open(
    "ingestion/moj_statistical_publications_source/publication_collection_mappings.yaml",
    "r",
) as f:
    ID_TO_DOMAIN_CONTACT_MAPPINGS = yaml.safe_load(f)


class MojPublicationsAPIParams(BaseModel):
    """various parameters to be passed to the API"""

    filter_organisations: list = Field(
        description="A list of orgs to get data for", examples=["ministry-of-justice"]
    )
    filter_content_store_document_type: list = Field(
        description="The type of document to filter by",
        examples=["national_statistics", "official_statistics"],
    )


class MojPublicationsAPIConfig(StatefulIngestionConfigBase):
    base_url: str = Field(description="URL to gov.uk search API")

    default_contact_email: str | None = Field(
        description="""
            The contact team email will default to this email if we haven't
            been able to populate a contact for a publication collection
            """,
        default=None,
    )
    collections_to_exclude: list = Field(
        description="list of the slug values for collections to exclude",
        examples=["publication-123"],
        default=[],
    )
    access_requirements: str = Field(
        description="""
            Paragraph explaning whether there are any specific access requirements related these data.
            gov.uk data being published can have a blanket para but needs to be different from the
            default section in find-moj-data""",
        default="",
    )
    params: MojPublicationsAPIParams = Field(
        description="parameters to be passed to the API",
        examples=[
            MojPublicationsAPIParams(
                filter_organisations=["ministry-of-justice"],
                filter_content_store_document_type=["national_statistics"],
            )
        ],
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        description="""
            Can configure whether the ingestion is be be stateful and can remove stale metadata.
            see https://datahubproject.io/docs/metadata-ingestion/docs/dev_guides/stateful/#stale-entity-removal
            """,
        default=None,
    )
