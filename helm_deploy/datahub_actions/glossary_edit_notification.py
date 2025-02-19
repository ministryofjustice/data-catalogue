import logging
from typing import Optional

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.transform.transformer import Transformer

logger = logging.getLogger(__name__)


class CustomTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return cls(config_dict, ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx
        self.counter = 0

    def transform(self, event: EventEnvelope) -> Optional[EventEnvelope]:
        # Simply print the received event.
        logger.info(event)
        # And return the original event (no-op)

        logger.info(event.event_type)

        logger.info(event.meta.keys())
        logger.info(event.event.as_json())

        self.counter += 1

        logger.info("filtered %d events", self.counter)

        return None
