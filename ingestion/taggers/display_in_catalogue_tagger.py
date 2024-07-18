import logging
from typing import List

import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import TagAssociationClass


def add_display_in_catalogue_tag(entity_urn: str) -> List[TagAssociationClass]:
    """
    Transformer to add dc_display_in_catalogue tag to all ingested entities
    other than the athena entities created via a dbt cadet ingestion
    """
    if "athena_cadet" not in entity_urn:
        tag_urn = builder.make_tag_urn(tag="dc_display_in_catalogue")
        tags = [TagAssociationClass(tag=tag_urn)]

        logging.info(f"Tagging dataset {entity_urn} with {tags}.")
    else:
        tags = []
    return tags
