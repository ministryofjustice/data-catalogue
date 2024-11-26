import os
from datetime import datetime, timedelta, timezone

import boto3
import yaml


def inject_run_result_paths_into_yaml_template(yaml_path):
    with open(yaml_path) as f:
        template = yaml.safe_load(f)

    template["source"]["config"]["run_results_paths"] = get_cadet_run_result_paths()

    # Overwite the original file with updated the run results paths.
    with open(yaml_path, "w") as f:
        yaml.dump(template, f, indent=2, sort_keys=False, default_flow_style=False)


def get_cadet_run_result_paths(bucket_name="mojap-derived-tables", days=1):
    """
    Find all keys in an S3 bucket that have a 'run_results.json' file.
    for last given amount of days
    """
    s3_client = boto3.client("s3")
    keys_with_run_results = []
    date_to_return = datetime.now(timezone.utc) - timedelta(days=days)
    paginator = s3_client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(
        Bucket=bucket_name, Prefix="prod/run_artefacts/"
    )

    for page in response_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                last_modified = obj["LastModified"]
                if key.endswith("run_results.json") and last_modified >= date_to_return:
                    keys_with_run_results.append(
                        os.path.join("s3://", bucket_name, key)
                    )

    return keys_with_run_results


if __name__ == "__main__":
    inject_run_result_paths_into_yaml_template("ingestion/cadet.yaml")
