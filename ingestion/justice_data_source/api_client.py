import requests
from ..dbt_manifest_utils import format_domain_name


# These map the api ids to domains as set by create_cadet_database_source
ID_TO_DOMAIN_MAPPING = {
    "prisons": "prison",
    "probation": "probation",
    "courts": "courts",
    "electronic-monitoring": "electronic monitoring",
    "electronic-monitoring-performance": "electronic monitoring",
    "bass": "probation",
    "cjs-crime": "general",
    "cjs-reoffending": "general",
    "cjs-sentence-types": "courts",
    "cjs-entrants": "courts",
}


class JusticeDataAPIClient:
    def __init__(self, base_url):
        self.session = requests.Session()
        self.base_url = base_url

    def list_all(self):
        """
        Traverse the metadata graph and return only leaf nodes.
        """
        leaf_nodes = {}
        to_process = []
        to_process.extend(self.session.get(self.base_url).json()["children"])
        while to_process:
            current = to_process.pop()
            id = current.get("id")

            # we are not ingesting justice-in-numbers - mainly repeated measures from other
            # sections, plus a couple which are difficult to assign a domain as they're not MOJ
            if id == "justice-in-numbers":
                continue

            if ID_TO_DOMAIN_MAPPING.get(id):
                domain = format_domain_name(ID_TO_DOMAIN_MAPPING.get(id, ""))
            elif not current.get("is_child", False):
                domain = None

            current["domain"] = domain
            breadcrumb = current.get("breadcrumb", []).copy()
            breadcrumb.append(current["name"])

            if current["children"] and current["children"] != [None]:
                for child in current["children"]:
                    child["breadcrumb"] = breadcrumb
                    child["is_child"] = True
                    child["domain"] = domain

                to_process.extend(current["children"])
            else:
                leaf_nodes[current["apiUrl"]] = current

        return list(leaf_nodes.values())
