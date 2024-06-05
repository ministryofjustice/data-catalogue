from datahub.configuration.common import ConfigModel
from pydantic import Field


class CreateDerivedTableDomainsConfig(ConfigModel):
    manifest_local_path: str = Field(
        description="local file path to dbt manifest json", default=None
    )
    manifest_s3_uri: str = Field(
        description="s3 path to dbt manifest json", default=None
    )
