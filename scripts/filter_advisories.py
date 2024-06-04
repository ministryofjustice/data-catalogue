import json
import re
import sys
from datetime import datetime
from typing import Any, Dict, List

import semantic_version


def to_snake_case(s: str) -> str:
    """Convert a string to snake_case."""
    return re.sub(r"\W|^(?=\d)", "_", s).lower()


def read_advisories(filename: str) -> List[Dict[str, Any]]:
    """Load advisories from a JSON file."""
    with open(filename, "r") as f:
        return json.load(f)


def parse_version_string(version_string: str) -> semantic_version.Version:
    """Parse semanatic version from provided version string"""
    version_str: str = re.sub(r".?v\s?(\d[.])", r"\1", version_string).strip()
    version = semantic_version.Version(version_str)

    return version


class ValidationError(Exception):
    def __init__(self, msg):
        self.msg = msg


def parse_vulnerabilities(
    advisory: Dict[str, Any],
    filtered: List[Dict[str, Any]],
    current_version: semantic_version.Version,
) -> None:
    for vulnerability in advisory.get("vulnerabilities", []):
        vulnerable_range: str = vulnerability.get("vulnerable_version_range", "")

        if not vulnerable_range:
            raise ValidationError(
                f"`vulnerabilities.vulnerable_version_range` field missing for {advisory['ghsa_id']}"
            )

        try:
            if "all" in vulnerable_range.lower():
                filtered.append(advisory)
                break

            for vuln in vulnerable_range.split(","):
                parsed_vulnerable_range = re.sub(r"v(\d[.])", r"\1", vuln).replace(
                    " ", ""
                )

                range_spec = semantic_version.NpmSpec(parsed_vulnerable_range)

                if current_version in range_spec:
                    filtered.append(advisory)
                    break

        except ValueError:
            filtered.append(advisory)
            break


def filter_advisories(
    advisories: List[Dict[str, Any]],
    current_version: semantic_version.Version,
    last_run_date: datetime,
) -> List[Dict[str, Any]]:
    """Filter advisories based on the current version and publication date."""
    filtered = []
    for advisory in advisories:
        published_date: str = advisory.get("published_at", "")

        if not published_date:
            raise ValidationError(
                f"`published_at` field missing for {advisory['ghsa_id']}"
            )

        published_at: datetime = datetime.fromisoformat(published_date)

        if published_at > last_run_date:
            parse_vulnerabilities(advisory, filtered, current_version)

    return filtered


def advisory_to_slack_block(advisory) -> tuple[dict[str, Any], bool]:
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


def format_slack_output(filtered_advisories) -> dict[str, list[Any]]:
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
                        "text": ":warning: contains _high severity_ advisory :warning:",
                    },
                },
            )
            slack_blocks.insert(1, {"type": "divider"})
            high_severity = block_severity

    return {"blocks": slack_blocks}


def main():
    # Load advisories
    advisories = read_advisories("advisories.json")

    # Define the current version to compare against
    # Set default last run date to the year 2000 if not provided or is blank
    if (not sys.argv[2].strip()) or (len(sys.argv) < 3):
        last_run_datetime_str = "2000-01-01T00:00:00Z"
    else:
        last_run_datetime_str: str = sys.argv[2].strip()

    current_version_str: str = sys.argv[1]
    current_version = parse_version_string(current_version_str)

    last_run_date: datetime = datetime.fromisoformat(last_run_datetime_str)

    if not current_version:
        print(f"Invalid current version: {current_version_str}")
        sys.exit(1)

    # Filter advisories
    filtered_advisories = filter_advisories(advisories, current_version, last_run_date)

    output = format_slack_output(filtered_advisories)

    with open("filtered_advisories.json", "w") as f:
        json.dump(output, f, indent=2)

    # Output the number of found advisories for GitHub Actions
    print(f"::set-output name=found_advisories::{len(filtered_advisories)}")


if __name__ == "__main__":
    main()
