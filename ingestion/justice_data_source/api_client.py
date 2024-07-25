import logging
import os
from datetime import datetime

import requests

from ..dbt_manifest_utils import format_domain_name
from .config import ID_TO_DOMAIN_MAPPING


class JusticeDataAPIClient:
    def __init__(self, base_url):
        self.session = requests.Session()
        self.base_url = base_url
        self.publication_details: list[dict] = self.session.get(
            os.path.join(self.base_url, "publications")
        ).json()

    def list_all(self, exclude_id_list: list = []):
        """
        Traverse the metadata graph and return only leaf nodes.
        """
        leaf_nodes = {}
        to_process = []
        to_process.extend(self.session.get(self.base_url).json()["children"])
        logging.info(f"ids in exclusion list: {exclude_id_list}")
        while to_process:
            current = to_process.pop()
            id = current.get("id")

            if id in exclude_id_list:
                continue

            if ID_TO_DOMAIN_MAPPING.get(id):
                domain = format_domain_name(ID_TO_DOMAIN_MAPPING.get(id, ""))
            elif not current.get("is_child", False):
                domain = None

            current["domain"] = domain
            breadcrumb = current.get("breadcrumb", []).copy()
            breadcrumb.append(current["name"])

            publication_id = current.get("dataPublicationId")

            if publication_id:
                last_updated, refresh_frequency = self._get_publication_timings(
                    publication_id
                )
                # datahub requires last updated to be an int or None if not known
                current["last_updated_timestamp"] = (
                    int(last_updated) if last_updated else None
                )
                # This is loaded as a custom property in datahub and so needs to be set as a string (even if empty)
                current["refresh_frequency"] = (
                    refresh_frequency if refresh_frequency else ""
                )

            if current["children"] and current["children"] != [None]:
                for child in current["children"]:
                    child["breadcrumb"] = breadcrumb
                    child["is_child"] = True
                    child["domain"] = domain

                to_process.extend(current["children"])
            else:
                leaf_nodes[current["apiUrl"]] = current

        return list(leaf_nodes.values())

    def _get_publication_timings(self, id: str) -> tuple[float | None, str | None]:
        """
        returns tuple of (last_updated, refresh_frequency), the current published date
        (as a timestamp) and publication frequency for the source publication of
        the chart id given as an input
        """
        last_updated_timestamp, refresh_frequency = None, None
        for publication in self.publication_details:
            if publication.get("id") == id:
                refresh_frequency = publication.get("frequency")
                try:
                    last_updated_timestamp = datetime.strptime(
                        publication.get("currentPublishDate", ""), "%d %B %Y"
                    ).timestamp()
                except (ValueError, TypeError) as e:
                    logging.warning(
                        f"Chart with id: {id}, missing valid currentPublishDate. Error: {e}"
                    )
                    last_updated_timestamp = None

        return last_updated_timestamp, refresh_frequency
