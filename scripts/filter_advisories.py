import csv
import json
import re
import sys
from datetime import datetime
from typing import Any, Dict, List

import semantic_version


def to_snake_case(s: str) -> str:
    """Convert a string to snake_case."""
    return re.sub(r"\W|^(?=\d)", "_", s).lower()


def parse_advisories(filename: str) -> List[Dict[str, Any]]:
    """Load advisories from a JSON file."""
    with open(filename, "r") as f:
        return json.load(f)


def filter_advisories(
    advisories: List[Dict[str, Any]],
    minimal_version: semantic_version.Version,
    last_run_date: datetime,
) -> List[Dict[str, Any]]:
    """Filter advisories based on the minimal vulnerable version and publication date."""
    filtered = []
    for advisory in advisories:
        published_at = datetime.fromisoformat(advisory.get("published_at", ""))
        if published_at > last_run_date:
            for vulnerability in advisory.get("vulnerabilities", []):
                vulnerable_range = vulnerability.get("vulnerable_version_range", "")
                if vulnerable_range:
                    try:
                        if vulnerable_range == "ALL":
                            filtered.append(advisory)
                            break
                        parsed_vulnerable_range = re.sub(
                            "v(\\d[.])", "\\1", vulnerable_range
                        ).replace(" ", "")
                        range_spec = semantic_version.NpmSpec(parsed_vulnerable_range)
                        if minimal_version in range_spec:
                            filtered.append(advisory)
                            break
                    except ValueError:
                        filtered.append(advisory)
                        break
    return filtered


def advisory_to_slack_block(advisory):
    severity = advisory["severity"]
    high_severity = False
    if severity in ["high", "critical"]:
        severity = f":alert: *{severity}* :alert:"
        high_severity = True
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"New <https://github.com/datahub-project/datahub/security/advisories|DataHub Security Advisory>:\n"
            f"*ID:* {advisory['ghsa_id']}\n"
            f"*Severity:* {severity}\n"
            f"*Published:* {advisory['published_at']}\n"
            f"*Summary:* {advisory['summary']}\n"
            f"*Vulnerable Versions:* {';'.join([v['vulnerable_version_range'] for v in advisory.get('vulnerabilities', [])])}\n"
            f"*Patched Versions:* {';'.join([v['patched_versions'] for v in advisory.get('vulnerabilities', [])])}\n"
            f"*Advisory:* {advisory['html_url']}\n",
        },
    }, high_severity


def main():
    # Load advisories
    advisories = parse_advisories("advisories.json")

    # Define the minimal version to compare against
    # Set default last run date to the year 2000 if not provided
    if len(sys.argv) < 3:
        last_run_date_str = "2000-01-01T00:00:00Z"
    else:
        last_run_date_str = sys.argv[2]
    minimal_version_str = re.sub("v(\\d[.])", "\\1", sys.argv[1])
    minimal_version = semantic_version.Version(minimal_version_str)
    last_run_date = datetime.fromisoformat(last_run_date_str)

    if not minimal_version:
        print(f"Invalid minimal version: {minimal_version_str}")
        sys.exit(1)

    # Filter advisories
    filtered_advisories = filter_advisories(advisories, minimal_version, last_run_date)

    slack_blocks = []
    high_severity = False
    for advisory in filtered_advisories:
        if slack_blocks:
            slack_blocks.append({"type": "divider"})
        advisory_block, block_severity = advisory_to_slack_block(advisory)
        slack_blocks.append(advisory_block)
        if block_severity and not high_severity:
            slack_blocks.insert(
                0,
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":alert: contains _high severity_ advisory :alert:",
                    },
                },
            )
            slack_blocks.insert(1, {"type": "divider"})
            high_severity = block_severity

    output = {"blocks": slack_blocks}

    with open("filtered_advisories.json", "w") as f:
        json.dump(output, f, indent=2)

    # Output the number of found advisories for GitHub Actions
    print(f"::set-output name=found_advisories::{len(filtered_advisories)}")


if __name__ == "__main__":
    main()
