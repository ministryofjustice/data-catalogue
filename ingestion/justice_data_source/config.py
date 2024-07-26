from datahub.configuration.common import ConfigModel
from pydantic import Field

# These map the api ids to domains as set by create_cadet_database_source
# and all children of these ids will inherit their parent's domain
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


class JusticeDataAPIConfig(ConfigModel):
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
