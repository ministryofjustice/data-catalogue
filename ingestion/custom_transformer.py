# custom_transformer.py
from datahub_actions.transform.transformer import Transformer
from datahub_actions.event.event import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from typing import Optional

class CustomTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        # Simply print the config_dict.
        print(config_dict)
        return cls(config_dict, ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def transform(self, event: EventEnvelope) -> Optional[EventEnvelope]:
        # Simply print the received event.
        print(event)
        # And return the original event (no-op)
        return event
