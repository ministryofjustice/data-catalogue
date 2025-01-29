import logging
import os

PLATFORM = "dbt"
# this needs to match the platform_instance value in cadet.yaml dbt recipe
# minus the .awscatalog bit
INSTANCE = os.getenv("CADET_INSTANCE")
if not INSTANCE:
    logging.warning("CADET_INSTANCE not set in github workflows, defaulting to 'cadet.awsdatacatalog'")
    INSTANCE = "cadet.awsdatacatalog"
ENV = "PROD"
