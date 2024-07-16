import requests


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
            breadcrumb = current.get("breadcrumb", []).copy()
            breadcrumb.append(current["name"])
            if current["children"] and current["children"] != [None]:
                for child in current["children"]:
                    child["breadcrumb"] = breadcrumb

                to_process.extend(current["children"])
            else:
                leaf_nodes[current["apiUrl"]] = current

        return list(leaf_nodes.values())
