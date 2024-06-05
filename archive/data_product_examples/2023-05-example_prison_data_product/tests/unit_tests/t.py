import os
from pathlib import Path

print(
    os.path.join(
        Path(__file__).parent.absolute(), "test_metadata", "02-data-dictionary.yaml"
    )
)
