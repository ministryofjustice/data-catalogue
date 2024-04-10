from pathlib import Path
import os

print(os.path.join(Path(__file__).parent.absolute(),
      "test_metadata", "02-data-dictionary.yaml"))
