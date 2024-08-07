import os

from locust import HttpUser, task

TEST_TOKEN = str(os.environ.get("DATAHUB_TEST_AUTH_TOKEN")).split(" ")


class APIUser(HttpUser):
    @task
    def search_datahub(self):

        query = {
            "query": "query Search($query: String!, $count: Int!, $start: Int!, $types: [EntityType!], $filters: [FacetFilterInput!], $sort: SearchSortInput) { searchAcrossEntities(input: { types: $types, query: $query, start: $start, count: $count, orFilters: [{ and: $filters }], sortInput: $sort }) { start, count, total, facets { field, displayName, aggregations { value, count, entity { ... on Domain { properties { name } }, ... on Tag { properties { name } }, ... on GlossaryTerm { properties { name } } } } }, searchResults { insights { text }, matchedFields { name, value }, entity { type, ... on Chart { urn, type, platform { name }, ownership { owners { owner { ... on CorpUser { urn, properties { fullName, email } }, ... on CorpGroup { urn, properties { displayName, email } } } } }, properties { name, description, externalUrl, customProperties { key, value } } }, ... on Dataset { urn, type, platform { name }, subTypes { typeNames }, relationships(input: { types: [\"DataProductContains\"], direction: INCOMING, count: 10 }) { total, relationships { entity { urn, ... on DataProduct { properties { name } } } } }, ownership { owners { owner { ... on CorpUser { urn, properties { fullName, email } }, ... on CorpGroup { urn, properties { displayName, email } } } } }, name, properties { name, qualifiedName, description, customProperties { key, value }, created, lastModified { time, actor } }, editableProperties { description }, tags { tags { tag { urn, properties { name, description } } } }, lastIngested, domain { domain { urn, id, properties { name, description } } } }, ... on DataProduct { urn, type, ownership { owners { owner { ... on CorpUser { urn, properties { fullName, email } }, ... on CorpGroup { urn, properties { displayName, email } } } } }, properties { name, description, customProperties { key, value }, numAssets }, domain { domain { urn, id, properties { name, description } } }, tags { tags { tag { urn, properties { name, description } } } } }, ... on Container { urn, type, subTypes { typeNames }, ownership { owners { owner { ... on CorpUser { urn, properties { fullName, email } }, ... on CorpGroup { urn, properties { displayName, email } } } } }, properties { name, description, customProperties { key, value } }, domain { domain { urn, id, properties { name, description } } }, tags { tags { tag { urn, properties { name, description } } } } } } } } }",
            "variables": {
                "query": "*",
                "count": 5,
                "start": 0,
            }
        }

        result = self.client.post("/api/graphql", json=query, headers={"authorization": f'Bearer {TEST_TOKEN}'})
        print(result.status_code, result.text)


