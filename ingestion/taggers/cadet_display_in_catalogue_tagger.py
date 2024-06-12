import logging
from typing import List

import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import TagAssociationClass


def add_display_in_catalogue_tag(entity_urn: str) -> List[TagAssociationClass]:
    """Compute the tags to associate to a given dataset."""
    if "athena_cadet" not in entity_urn:
        tag_urn = builder.make_tag_urn(tag="dc_display_in_catalogue")
        tags = [TagAssociationClass(tag=tag_urn)]

        logging.info(f"Tagging dataset {entity_urn} with {tags}.")
    else:
        tags = []
    return tags
