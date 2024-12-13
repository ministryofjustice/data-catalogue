# Datahub metadata ingestions
Here is where we have the configuration files ([recipes](https://datahubproject.io/docs/metadata-ingestion/recipe_overview/)) for the majority of our ingestions into Datahub (run on a daily schedule for preprod and prod) and also the code we've written for our custom ingestion sources, those that are specific to us and not natively supported by Datahub.

We have some other metadata in [data-catalogue-metadata](https://github.com/ministryofjustice/data-catalogue-metadata), however this is an internal repository and has currently been used for ingestions we've not scheduled and do not have as robust a way to automate the ingestion pipeline.

These github action workflows ingest the data covered below into each environment:
- [ingest-prod.yml](.github/workflows/ingest-prod.yml) - Ingests into prod and runs daily at 7:15am UTC
- [ingest-preprod.yml](.github/workflows/ingest-preprod.yml) - Ingests into preprod and runs daily at 9:00am UTC
- [ingest-test.yml](.github/workflows/ingest-test.yml) - Ingests into test and needs to be manually triggered
- [ingest-dev.yml](.github/workflows/ingest-dev.yml) - Ingests into dev and needs to be manually triggered

prod and preprod workflows uses different github environments to avoid needing approval on each workflow that runs. These are `prod-ingestion` and `preprod-ingestion`

</br>

## Create a Derived Table (Cadet) ingestion

This uses Datahub's native [dbt ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/dbt/) although we have had to supplement this with some custom code to create container (database) entities in datahub and assign the ingested dbt models to these databases.

The cadet ingestion has the following components

### Cadet databases
A custom ingestion [source](ingestion/create_cadet_databases_source/source) and [config](ingestion/create_cadet_databases_source/config) used to create database entities for every dbt model entity, and then the database is tagged to with `dc_display_in_catalogue` if one or more of its children has that tag also.

This custom code is required because the dbt manifest file, used to ingest the metadata from, does not contain any metadata at the database level and the Datahub dbt ingestion ingests at the model level (which can be considered equivalent to a database table) without creating database entities

In the simplest terms this ingestion infers parent databases for the entities within the manifest and creates the container entities of subtype databases within datahub. 

It also assigns all cadet tables to domains in datahub.

The recipe file for this component can be found [here](ingestion/create_cadet_databases.yaml)

**It does not assign cadet tables to databases**

### Cadet tables
This handles the main bulk of the cadet metadata and depends on Datahub's native [dbt ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/dbt/). 

It creates dataset entities in two platforms within datahub:
1. `dbt` - this is the main Platform, defaulted to by datahub and cannot be changed. These dataset entities are of subtype `Model` or `View`.
2. `Athena` - this is the target platform and is set in the ingestion recipe. These dataset entities are of subtype `Table`.

There is a sibling relationship created for these 2 entities in datahub, but for the purpose of Find MoJ data we only display the entities from the `dbt` platform. This is done by tagging only the `dbt` entities with `dc_display_in_catalogue`. And because users may not be familiar with the terminalogy used in dbt we display `Model` and `View` dataset entities from the `dbt` platform as `Table` in Find MoJ data.

## Cadet transformers
We have also developed a [custom transformer](https://datahubproject.io/docs/actions/guides/developing-a-transformer/) for this ingestion, alongside transformer that datahub provides. Transformers are a way of adding metadata to entities during an ingestion see [datahub transfomrer definition](https://datahubproject.io/docs/metadata-ingestion/docs/transformer/intro/) .

Transformers used are:
- [assign_cadet_databases](ingestion/transformers/assign_cadet_databases.py) - Custom transformer. Assigns each ingested dataset entity to its parent database
- simple_add_dataset_properties - Datahub provided transformer. Adds the `audience` custom property to all ingested entities.

The recipe file for this component can be found [here](ingestion/cadet.yaml)

### Cadet ingestion workflow
The workflow for the cadet ingestion can be found [here](.github/workflows/ingest-cadet-metadata.yml)

</br>

## Glue ingestion
For these data we use Datahub's native [Glue ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/glue/).

### Glue databases and tables
We define a recipe file for glue databases in an area, they'll be prefixed `glue_`, eg. [glue_sop.yaml](ingestion/glue_sop.yaml). The recipe may contain several databases but they will have common metadata properties set, such as data custodian and domain.

### Glue transformers
We have defined a transformer to add some additional metadata properties to the created entities.

Transformers used are:
- [enrich_container_transformer](ingestion/transformers/enrich_container_transformer.py) - Custom transformer. Adds an owner, domain, and tag for a provided container.
- simple_add_dataset_tags - Datahub provided transformer. Adds the `dc_display_in_catalogue` tag to all ingested entities.
- simple_add_dataset_domain - Datahub provided transformer. Adds the given domain to all ingested entities.
- simple_add_dataset_ownership - Datahub provided transformer. Adds the given owner to all ingested entities.

### Glue ingestion workflow
The workflow for the Glue ingestion can be found [here](.github/workflows/ingest-glue-data.yml)

</br>

## Justice data ingestion

### Justice data dashboard and charts
We have developed a [custom ingestion source for Justice Data](ingestion/justice_data_source/source.py). It uses [this python script](ingestion/justice_data_source/api_client.py) to fetch data from the Justice Data public API parsing the metadata into a format which can be used by the source.py file to create each metadata aspect for the Chart entities it creates. It also attaches the chart entities it creates into a single dashboard entity called Justice Data. Here is the [config](ingestion/justice_data_source/config.py) developed for the justice data ingestion to be used in the recipe.

It loads all entities into a custom platform called `justice-data`

[see the recipe](ingestion/justice_data_ingest.yaml) for this ingestion. 

### Justice data ingestion workflow
The workflow for the Glue ingestion can be found [here](.github/workflows/ingest-justice-data.yml)

</br>

## GOV.UK statistical publications ingestion

### GOV.UK statistical publication collections and datasets
We have developed a [custom ingestion source for GOV.UK publications](ingestion/moj_statistical_publications_source/source.py). It uses [this python script](ingestion/moj_statistical_publications_source/api_client.py) to fetch data from the GOV.UK public APIs (search and content) parsing the metadata into a format which can be used by the source.py file to create each metadata aspect for the Publication collection and Publication datasets entities it creates. Where Publication collection is a container for Publication Datasets. Here is the [config](ingestion/moj_statistical_publications_source/config.py) developed for the publications ingestion to be used in the recipe.

It loads all entities into a custom platform called `GOV.UK`

This ingestion also has a mapping yaml file which maps publication collections to domains and team contact emails. Publication datasets inherit the domain and contact details from their parent collection.

[see the recipe](ingestion/moj_publications.yaml) for this ingestion. 

### GOV.UK statistical publications ingestion workflow
The workflow for the Glue ingestion can be found [here](.github/workflows/ingest-moj-publications.yml)
