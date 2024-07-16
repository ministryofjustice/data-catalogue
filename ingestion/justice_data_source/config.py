from datahub.configuration.common import ConfigModel
from pydantic import Field


class JusticeDataAPIConfig(ConfigModel):
    base_url: str = Field(description="URL to justice data API")
