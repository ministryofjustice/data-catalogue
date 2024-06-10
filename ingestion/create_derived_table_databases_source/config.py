from datahub.configuration.common import ConfigModel
from pydantic import Field


class CreateDerivedTableDatabasesConfig(ConfigModel):
    manifest_s3_uri: str = Field(
        description="s3 path to dbt manifest json", default=None
    )
