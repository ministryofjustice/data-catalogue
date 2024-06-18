import os

from locust import HttpUser, between, task
import random

SESSION_ID = os.environ.get("SESSION_ID")


class APIUser(HttpUser):
    table_names = {
        'absconds', 'guards', 'cellblocks', 'visits', 'contraband', 'parole',
        'warden', 'transfers', 'sentences', 'offenses', 'rehabilitation', 'programs',
        'incidents', 'facilities', 'monitoring', 'restrictions', 'commissary',
        'healthcare', 'legal', 'records'
    }

    @task
    def search_random(self):
        headers = {
            'Cookie': f"sessionid={SESSION_ID}"}
        self.client.get(f"/search?query={random.choice(list(self.table_names))}&domain=&sort=relevance", headers=headers)
