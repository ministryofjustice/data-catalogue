import logging
from datetime import datetime
import requests

from .config import ID_TO_SUBJECT_AREAS_MAPPING


class JusticeDataAPIClient:
    def __init__(self, base_url, default_owner_email):
        self.session = requests.Session()
        self.base_url = base_url
        self.publication_details: dict[str, dict] = {
            pub["id"]: pub for pub in self.list_publications()
        }
        self.default_owner_email = default_owner_email
        self._id_to_subject_areas_mapping = ID_TO_SUBJECT_AREAS_MAPPING

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

            if self._id_to_subject_areas_mapping.get(id):
                subject_areas = self._id_to_subject_areas_mapping.get(id, "")
            elif current.get("subject_areas"):
                subject_areas = current["subject_areas"]
            else:
                subject_areas = ["General"]

            current["subject_areas"] = subject_areas
            breadcrumb = current.get("breadcrumb", []).copy()
            breadcrumb.append(current["name"])

            publication_id = current.get("dataPublicationId")

            if publication_id:
                last_updated, refresh_period, owner_email = (
                    self._get_publication_metadata(publication_id)
                )

                current["last_updated_timestamp"] = last_updated
                current["refresh_period"] = refresh_period
                current["owner_email"] = owner_email
            else:
                current["owner_email"] = self.default_owner_email

            if current["children"] and current["children"] != [None]:
                for child in current["children"]:
                    child["breadcrumb"] = breadcrumb
                    child["subject_areas"] = subject_areas

                to_process.extend(current["children"])
            else:
                leaf_nodes[current["apiUrl"]] = current

        return list(leaf_nodes.values())

    def _get_publication_metadata(
        self, id: str
    ) -> tuple[datetime | None, str | None, str]:
        """
        returns tuple of (last_updated, refresh_period, owner_email), the current published date
        (as a timestamp), publication frequency and owner email for the source publication of
        the chart id given as an input
        """
        publication = self.publication_details.get(id)
        if not publication:
            return (None, None, self.default_owner_email)

        refresh_period = publication.get("frequency")
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

        return current_publish_date, refresh_period, owner_email

    def validate_subject_areas(self, top_level_subject_areas: list[str]) -> bool:
        for subject_areas in self._id_to_subject_areas_mapping.values():
            # This will check if any of the subject areas are in the top level subject areas
            if not any(set(top_level_subject_areas).intersection(set(subject_areas))):
                raise ValueError(
                    f"Subject areas {subject_areas} are not in the top level subject areas {top_level_subject_areas}"
                )
        return True
