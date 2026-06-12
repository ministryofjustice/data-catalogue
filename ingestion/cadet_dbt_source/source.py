import logging

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class
from datahub.ingestion.source.dbt.dbt_common import DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource

from ingestion.ingestion_utils import is_excluded_name

logger = logging.getLogger(__name__)


@config_class(DBTCoreConfig)
class CadetDBTSource(DBTCoreSource):
    def __init__(self, config: DBTCoreConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCoreConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _is_allowed_node(self, node: DBTNode) -> bool:
        if not super()._is_allowed_node(node):
            return False

        excluded_values = {
            "dbt_name": node.dbt_name,
            "database": node.database,
            "schema": node.schema,
            "name": node.name,
            "alias": node.alias,
        }

        for field_name, value in excluded_values.items():
            if is_excluded_name(value):
                logger.info(
                    "Skipping dbt node %s because %s matched an excluded keyword: %s",
                    node.dbt_name,
                    field_name,
                    value,
                )
                return False

        return True