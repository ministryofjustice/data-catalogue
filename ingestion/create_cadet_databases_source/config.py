from datahub.configuration.common import ConfigModel
from pydantic import Field


class CreateCadetDatabasesConfig(ConfigModel):
    manifest_s3_uri: str = Field(
        description="s3 path to dbt manifest json", default=None
    )
    database_metadata_s3_uri: str = Field(
        description="s3 path to database_metadata json", default=None
    )
