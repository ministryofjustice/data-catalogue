import logging

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource

from ingestion.ingestion_utils import is_excluded_name

logger = logging.getLogger(__name__)

EXCLUDED_DATABASES = {"libra"}


def is_excluded_database(database: str | None) -> bool:
    if not database:
        return False

    return database.lower() in EXCLUDED_DATABASES


@config_class(DBTCoreConfig)
class CadetDBTSource(DBTCoreSource):
    def __init__(self, config: DBTCoreConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCoreConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def loadManifestAndCatalog(self):
        nodes, *metadata = super().loadManifestAndCatalog()

        # Hard-exclude selected databases from CaDeT ingestion.
        filtered_nodes = []
        excluded_count = 0
        for node in nodes:
            if is_excluded_database(node.schema):
                excluded_count += 1
                logger.info(
                    "Excluding dbt node %s from ingestion because schema '%s' is denied",
                    node.dbt_name,
                    node.schema,
                )
                continue
            filtered_nodes.append(node)

        if excluded_count:
            logger.info(
                "Excluded %d dbt nodes from ingestion by database denylist",
                excluded_count,
            )

        display_tag = f"{self.config.tag_prefix}dc_display_in_catalogue"
        for node in filtered_nodes:
            excluded_values = {
                "dbt_name": node.dbt_name,
                "database": node.database,
                "schema": node.schema,
                "name": node.name,
                "alias": node.alias,
            }

            for field_name, value in excluded_values.items():
                if is_excluded_name(value):
                    if display_tag in node.tags:
                        node.tags = [tag for tag in node.tags if tag != display_tag]
                        logger.info(
                            "Removing %s from dbt node %s because %s matched an excluded keyword: %s",
                            display_tag,
                            node.dbt_name,
                            field_name,
                            value,
                        )
                    break

        return (filtered_nodes, *metadata)