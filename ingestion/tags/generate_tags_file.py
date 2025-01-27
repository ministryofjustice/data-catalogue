"""
Generate a file that can be ingested with Datahub's file source.
The output is a sequence of serialised Metadata Change Events (MCEs).
Since this format is quite verbose this script generates it based on a simpler YAML structure.
"""

import json
from pathlib import Path

import yaml
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    TagPropertiesClass,
    TagSnapshotClass,
)

dir = Path(__file__).parent

output = []

with open(dir / "subject_areas_template.yaml") as file:
    for tag in yaml.safe_load(file):
        name = tag["name"]
        properties = TagPropertiesClass(name=name, description=tag["description"])
        urn = f"urn:li:tag:{name}"
        output.append(
            MetadataChangeEventClass(
                proposedSnapshot=TagSnapshotClass(urn=urn, aspects=[properties])
            ).to_obj()
        )

with open(dir / "subject_areas.json", "w") as file:
    json.dump(output, file, indent=2)
