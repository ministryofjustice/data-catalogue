import logging
from datetime import datetime

import requests

from ..ingestion_utils import format_domain_name
from .config import ID_TO_DOMAIN_MAPPING


class JusticeDataAPIClient:
    def __init__(self, base_url, default_owner_email):
        self.session = requests.Session()
        self.base_url = base_url
        self.publication_details: dict[str, dict] = {
            pub["id"]: pub for pub in self.list_publications()
        }
        self.default_owner_email = default_owner_email
        self._id_to_domain_mapping = ID_TO_DOMAIN_MAPPING

    def list_publications(self) -> dict:
        """
        Return a list of publications
        """
        return self.session.get(self.base_url + "/publications").json()

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

            if current.get("permalink") is None:
                # Skip anything without a permalink, e.g. jin-court-capacity-subhead
                logging.info(f"{id=} has no permalink and will be skipped.")
                continue

            if self._id_to_domain_mapping.get(id):
                domain = format_domain_name(self._id_to_domain_mapping.get(id, ""))
            elif current.get("domain"):
                domain = current["domain"]
            else:
                domain = "General"

            current["domain"] = domain
            breadcrumb = current.get("breadcrumb", []).copy()
            breadcrumb.append(current["name"])

            publication_id = current.get("dataPublicationId")

            if publication_id:
                last_updated, refresh_frequency, owner_email = (
                    self._get_publication_metadata(publication_id)
                )

                current["last_updated_timestamp"] = last_updated
                current["refresh_frequency"] = refresh_frequency
                current["owner_email"] = owner_email
            else:
                current["owner_email"] = self.default_owner_email

            if current["children"] and current["children"] != [None]:
                for child in current["children"]:
                    child["breadcrumb"] = breadcrumb
                    child["domain"] = domain

                to_process.extend(current["children"])
            else:
                leaf_nodes[current["apiUrl"]] = current

        return list(leaf_nodes.values())

    def _get_publication_metadata(
        self, id: str
    ) -> tuple[datetime | None, str | None, str]:
        """
        returns tuple of (last_updated, refresh_frequency, owner_email), the current published date
        (as a timestamp), publication frequency and owner email for the source publication of
        the chart id given as an input
        """
        publication = self.publication_details.get(id)
        if not publication:
            return (None, None, self.default_owner_email)

        refresh_frequency = publication.get("frequency")
        try:
            current_publish_date = datetime.strptime(
                publication.get("currentPublishDate", ""), "%d %B %Y"
            )
        except (ValueError, TypeError) as e:
            logging.warning(
                f"Chart with id: {id}, missing valid currentPublishDate. Error: {e}"
            )
            current_publish_date = None

        owner_email = publication.get("ownerEmail", self.default_owner_email)

        if owner_email is None:
            owner_email = self.default_owner_email

        return current_publish_date, refresh_frequency, owner_email

    def validate_domains(self, datahub_domains) -> bool:
        for domain in set(self._id_to_domain_mapping.values()):
            if domain.lower() not in set(datahub_domains):
                raise ValueError(
                    f"Domain - {domain} does not exist in datahub - please review domain mappings in config.py"
                )
        return True
