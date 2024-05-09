

#### EntityServiceImpl
- [A class specifying Create, Read, and Update operations against metadata entities and aspects by primary key (urn)](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-io/src/main/java/com/linkedin/metadata/entity/EntityServiceImpl.java#L107-L137)

  - **ingestAspectsToLocalDB** [Ingests (inserts) a new version of an entity aspect & emits a MetadataChangeLog (MCL)](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-io/src/main/java/com/linkedin/metadata/entity/EntityServiceImpl.java#L649C6-L671)

#### EntityResource

- [Single unified resource for fetching, updating, searching, & browsing DataHub entities](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L102)

  - **get** [Retrieves the value for an entity that is made up of latest versions of specified aspects](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L182)

  - **batchGet** [Retrieves the value for an entity that is made up of latest versions of specified aspects](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L182)

  - **ingest** [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L275)

  - **batchIngest** [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L317)

  - **search** [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L317)

    - FYI: ["This API is not used by the frontend for search bars so we default to structured"](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L404)
    - FYI: ["TODO - change it to use _searchService once we are confident on it's latency"](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L400)
    - **NOTE**: two actions can increment 'search'
        
        1. [Task<searchResult> search](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L375) ... [metric](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L419), context: [`"GET SEARCH RESULTS for {} with query {}"`](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L399)
        1. [Task<searchResult> filter](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L1157) ... [metric](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L1188), context: [`"FILTER RESULTS for {} with filter {}"`](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L1175)


    - No metric definitions:
      - [searchAcrossEntities](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L425)
      - [scrollAcrossEntities](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L468)
      - [searchAcrossLineage](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L525)
      - [scrollAcrossLineage](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L582)

  - **filter** [aka 'list'](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L646)

  - **autocomplete** [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L686)

  - **browse** [context: `"GET BROWSE RESULTS for {} at path {}"`](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L726)

  - **getBrowsePaths** [context: `"GET BROWSE PATHS for {}"`](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/restli-servlet-impl/src/main/java/com/linkedin/metadata/resources/entity/EntityResource.java#L686)

#### MetadataChangeLogProcessor

- [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeLogProcessor.java#L48)

  - **consume** [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeLogProcessor.java#L78)

  - Hooks:
      - Each is invoked by: [`"Invoking MCL hook {} for urn: {}"`](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeLogProcessor.java#L114-L126)

    - **[EntityChangeEventGeneratorHook](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeLogProcessor.java#L41)**

      - [Hook responsible for generating Entity Change Events to the Platform Events topic](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/hook/event/EntityChangeEventGeneratorHook.java#L40)

    - **[IngestionSchedulerHook](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeLogProcessor.java#L40)**

      - ["This hook updates a stateful IngestionScheduler of Ingestion Runs for Ingestion Sources defined within DataHub."](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/hook/ingestion/IngestionSchedulerHook.java#L25)
      
    - **[SiblingAssociationHook](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeLogProcessor.java#L43)**

      - ["This hook associates dbt datasets with their sibling entities"](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mae-consumer/src/main/java/com/linkedin/metadata/kafka/hook/siblings/SiblingAssociationHook.java#L51)

#### MetadataChangeEventsProcessor

- [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mce-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeEventsProcessor.java#L42)

#### MetadataChangeProposalsProcessor

- [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-jobs/mce-consumer/src/main/java/com/linkedin/metadata/kafka/MetadataChangeProposalsProcessor.java#L38)

#### ESSearchDAO
- [A search DAO for Elasticsearch backend.](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/query/ESSearchDAO.java#L62)

  - [searchResult](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/query/ESSearchDAO.java#L245)
    
    - [Gets a list of documents that match given search request. The results are aggregated and filters are applied to the search hits and not the aggregation results.](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/query/ESSearchDAO.java#L231)

  - [executeAndExtract](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/query/ESSearchDAO.java#L102)
    
    - [`"Executing request {}: {}"`](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/query/ESSearchDAO.java#L112)

datahub graphql

#### GraphQLController

- [definition](https://github.com/datahub-project/datahub/blob/c00ddb2a0d4fb7be9b506d09d5f015082ab9742d/metadata-service/graphql-servlet-impl/src/main/java/com/datahub/graphql/GraphQLController.java#L42)
  - type: "histograms"
  - [browseV2](https://datahubproject.io/docs/graphql/queries/#browsev2)
  - [batchGetStepStates](https://datahubproject.io/docs/graphql/queries/#batchgetstepstates)
  - [call](https://github.com/datahub-project/datahub/blob/6f020015010bd7acb12f31080f5dd2af1fb0254c/metadata-service/graphql-servlet-impl/src/main/java/com/datahub/graphql/GraphQLController.java#L207)
  - [error](https://github.com/datahub-project/datahub/blob/6f020015010bd7acb12f31080f5dd2af1fb0254c/metadata-service/graphql-servlet-impl/src/main/java/com/datahub/graphql/GraphQLController.java#L177)