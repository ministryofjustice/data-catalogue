# Datahub metadata ingestions

Here is where we have the configuration files ([recipes](https://datahubproject.io/docs/metadata-ingestion/recipe_overview/)) for the majority of our ingestions into Datahub (run on a daily schedule for preprod and prod) and also the code we've written for our custom ingestion sources, those that are specific to us and not natively supported by Datahub.

We have some other metadata in [data-catalogue-metadata](https://github.com/ministryofjustice/data-catalogue-metadata), however this is an internal repository and has currently been used for ingestions we've not scheduled and do not have as robust a way to automate the ingestion pipeline.

These github action workflows ingest the data covered below into each environment:

- [ingest-prod.yml](../.github/workflows/ingest-prod.yml) - Ingests into prod and runs on a daily schedule
- [ingest-preprod.yml](../.github/workflows/ingest-preprod.yml) - Ingests into preprod and runs on a daily schedule
- [ingest-test.yml](../.github/workflows/ingest-test.yml) - Ingests into test and needs to be manually triggered
- [ingest-dev.yml](../.github/workflows/ingest-dev.yml) - Ingests into dev and needs to be manually triggered

Prod and Preprod workflows use different Github environments to avoid needing approval on each workflow that runs. These are `prod-ingestion` and `preprod-ingestion`

---

## Create a Derived Table (Cadet) ingestion

This uses Datahub's native [dbt ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/dbt/) although we have had to supplement this with some custom code to create container (database) entities in datahub and assign the ingested dbt models to these databases.

The cadet ingestion has the following components

### Cadet databases

A custom ingestion [source](create_cadet_databases_source/source) and [config](create_cadet_databases_source/config) used to create database entities for every dbt model entity, and then the database is tagged to with `dc_display_in_catalogue` if one or more of its children has that tag also.

This custom code is required because the dbt manifest file, used to ingest the metadata from, does not contain any metadata at the database level and the Datahub dbt ingestion ingests at the model level (which can be considered equivalent to a database table) without creating database entities

In the simplest terms this ingestion infers parent databases for the entities within the manifest and creates the container entities of subtype databases within datahub.

It also tags all cadet tables with subject areas in datahub.

The recipe file for this component can be found [here](create_cadet_databases.yaml)

**It does not assign cadet tables to databases**

### Cadet tables

This handles the main bulk of the cadet metadata and depends on Datahub's native [dbt ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/dbt/).

It creates dataset entities in two platforms within datahub:

1. `dbt` - this is the main Platform, defaulted to by datahub and cannot be changed. These dataset entities are of subtype `Model` or `View`.
2. `Athena` - this is the target platform and is set in the ingestion recipe. These dataset entities are of subtype `Table`.

There is a sibling relationship created for these 2 entities in datahub, but for the purpose of Find MoJ data we only display the entities from the `dbt` platform. This is done by tagging only the `dbt` entities with `dc_display_in_catalogue`. And because users may not be familiar with the terminalogy used in dbt we display `Model` and `View` dataset entities from the `dbt` platform as `Table` in Find MoJ data.

### Cadet transformers

We have also developed a [custom transformer](https://datahubproject.io/docs/actions/guides/developing-a-transformer/) for this ingestion, alongside transformer that datahub provides. Transformers are a way of adding metadata to entities during an ingestion see [datahub transfomrer definition](https://datahubproject.io/docs/metadata-ingestion/docs/transformer/intro/) .

Transformers used are:

- [assign_cadet_databases](transformers/assign_cadet_databases.py) - Custom transformer. Assigns each ingested dataset entity to its parent database
- simple_add_dataset_properties - Datahub provided transformer. Adds the `audience` custom property to all ingested entities.

The recipe file for this component can be found [here](cadet.yaml)

### Cadet ingestion workflow

The workflow for the cadet ingestion can be found [here](../.github/workflows/ingest-cadet-metadata.yml)

---

## Glue ingestion

For these data we use Datahub's native [Glue ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/glue/).

### Glue databases and tables

We define a recipe file for glue databases in an area, they'll be prefixed `glue_`, eg. [glue_sop.yaml](glue_sop.yaml). The recipe may contain several databases but they will have common metadata properties set, such as data custodian and subject area.

### Glue transformers

We have defined a transformer to add some additional metadata properties to the created entities.

Transformers used are:

- [enrich_container_transformer](transformers/enrich_container_transformer.py) - Custom transformer. Adds an owner and tags for a provided container.
- simple_add_dataset_tags - Datahub provided transformer. Adds the `dc_display_in_catalogue` tag to all ingested entities.
- simple_add_dataset_ownership - Datahub provided transformer. Adds the given owner to all ingested entities.

### Glue ingestion workflow

The workflow for the Glue ingestion can be found [here](../.github/workflows/ingest-glue-data.yml)

---

## Justice data ingestion

### Justice data dashboard and charts

We have developed a [custom ingestion source for Justice Data](justice_data_source/source.py). It uses [this python script](justice_data_source/api_client.py) to fetch data from the Justice Data public API parsing the metadata into a format which can be used by the source.py file to create each metadata aspect for the Chart entities it creates. It also attaches the chart entities it creates into a single dashboard entity called Justice Data. Here is the [config](justice_data_source/config.py) developed for the justice data ingestion to be used in the recipe.

It loads all entities into a custom platform called `justice-data`

[see the recipe](justice_data_ingest.yaml) for this ingestion.

### Justice data ingestion workflow

The workflow for the Glue ingestion can be found [here](../.github/workflows/ingest-justice-data.yml)

---

## GOV.UK statistical publications ingestion

### GOV.UK statistical publication collections and datasets

We have developed a [custom ingestion source for GOV.UK publications](moj_statistical_publications_source/source.py). It uses [this python script](moj_statistical_publications_source/api_client.py) to fetch data from the GOV.UK public APIs (search and content) parsing the metadata into a format which can be used by the source.py file to create each metadata aspect for the Publication collection and Publication datasets entities it creates. Where Publication collection is a container for Publication Datasets. Here is the [config](moj_statistical_publications_source/config.py) developed for the publications ingestion to be used in the recipe.

It loads all entities into a custom platform called `GOV.UK`

This ingestion also has a mapping yaml file which maps publication collections to subject areas and team contact emails. Publication datasets inherit the subject areas and contact details from their parent collection.

[see the recipe](ingestion/moj_publications.yaml) for this ingestion.

### GOV.UK statistical publications ingestion workflow

The workflow for the Glue ingestion can be found [here](../.github/workflows/ingest-moj-publications.yml)

---

## Post ingestion checks

We have developed some checks to run post ingestion to monitor whether there have been any uncaught issues affecting the metadata after an ingestion and to be confident all datasets have retained their container relations (this is an error that we've experienced in dev and causes databases in fmd to appear to contain no tables).

The initial development essentially checks three things:

1. Whether any create a derived table datasets are missing an `IsPartOf` container relationship
2. Whether any owner or tag values are in prod but not preprod or in preprod but not prod (grouped by platform)
3. Whether the count of any entity type, owner or tag has a difference >20% comparing prod and preprod (grouped by platform)

These checks are run using the github actions workflow [post-ingestion-checks.yml](../.github/workflows/post-ingestion-checks.yml), with the code for the checks in [post_ingestion_checks.py](post_ingestion_checks.py).
