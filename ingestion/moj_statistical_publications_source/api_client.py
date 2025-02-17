import json
import logging
import os
from typing import Any

import requests

from .config import ID_TO_METADATA_MAPPINGS, MojPublicationsAPIParams


class MojPublicationsAPIClient:
    def __init__(self, base_url, default_contact_email, params):
        self.session = requests.Session()
        self.base_url = base_url
        self.params: MojPublicationsAPIParams = params
        self.default_contact_email = default_contact_email
        self._id_to_metadata_mapping = ID_TO_METADATA_MAPPINGS

    def list_all_publications_metadata(self):
        params_dict = dict(self.params)
        # These params are unlikely to need to be changed and if fields are then
        # we'll need to update the code to return any fields added
        params_dict.update(
            {
                "fields": [
                    "description",
                    "document_collections",
                    "link",
                    "public_timestamp",
                    "title",
                    "first_published_at",
                ],
                # Maximum results per API call is 1500
                "count": 1500,
                "start": 0,
            }
        )
        all_results = []
        while True:
            # Construct the search URL
            search_url = os.path.join(self.base_url, "search.json")
            # Make the request
            response = self.session.get(search_url, params=params_dict)

            # Check the status code
            if response.status_code == 200:
                # Store the result data in a JSON object
                result_data = json.loads(response.text)

                # Add the results to the overall list
                all_results.extend(result_data["results"])

                # Check if we've fetched all the results
                if len(all_results) >= result_data["total"]:
                    break

                # Update the start parameter for the next page
                params_dict["start"] += params_dict["count"]
            else:
                logging.error(f"Error: {response.status_code} - {response.text}")
                break
        return all_results

    def get_collections_from_all_results(
        self, all_results: list[dict], collections_to_exclude: list[str]
    ) -> list[dict[Any, Any]]:
        # Get all document collections. Note some documents are in multiple collections
        all_collections = [
            tuple(doc.items())
            for documents in all_results
            if documents.get("document_collections")
            for doc in documents["document_collections"]
        ]
        unique_collections = set(all_collections)

        unique_collections = [
            dict(collection)
            for collection in unique_collections
            if dict(collection).get("slug") not in collections_to_exclude
        ]

        for collection in unique_collections:
            # descriptions and last updated dates need to be fetched from the content API
            # for each collection
            link = collection.get("link")
            if not link:
                logging.error(f"{collection=} does not have a link")
                continue
            content_api_url = os.path.join(
                self.base_url, f"content/{link}"
            )
            content_response = self.session.get(content_api_url).json()
            collection["description"] = content_response.get("description")
            collection["subject_areas"] = self._id_to_metadata_mapping.get(
                collection["slug"], {}
            ).get("subject_areas")

            collection["last_updated"] = content_response.get("public_updated_at")
            collection["contact_email"] = self._id_to_metadata_mapping.get(
                collection["slug"], {}
            ).get("contact_email", self.default_contact_email)
        return unique_collections
