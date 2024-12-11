# Datahub metadata ingestions
Here is where we have the configuration files ([recipes](https://datahubproject.io/docs/metadata-ingestion/recipe_overview/)) for the majority of our ingestions into Datahub (run on a daily schedule for preprod and prod) and also the code we've written for our custom ingestion sources, those that are specific to us and not natively supported by Datahub.

We have some other metadata in [data-catalogue-metadata](https://github.com/ministryofjustice/data-catalogue-metadata), however this is an internal repository and has currently been used for ingestions we've not scheduled and do not have as robust a way to automate the ingestion pipeline.


## Create a Derived Table (Cadet) ingestion

This uses Datahub's native [dbt ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/dbt/) although we have had to supplement this with some custom code to create container (database) entities in datahub and assign the ingested dbt models to these databases.

The cadet ingestion has the following components

### Cadet databases
A custom ingestion [source](ingestion/create_cadet_databases_source/source) and [config](ingestion/create_cadet_databases_source/config) used to create database entities for every dbt model entity, and then the database is tagged to with `dc_display_in_catalogue` if one or more of its children has that tag also.

This custom code is required because the dbt manifest file, used to ingest the metadata from, does not contain any metadata at the database level and the Datahub dbt ingestion ingests at the model level (which can be considered equivalent to a database table) without creating database entities

In the simplest terms this ingestion infers parent databases for the entities within the manifest and creates the container entities of subtype databases within datahub. 

It was also updated to include assigning all cadet tables to domains in datahub. This was previously done via a transformer in the standard dbt ingestion but caused the ingestion to last 3 hours. This way is much more efficient.

The recipe file for this component can be found [here](ingestion/create_cadet_databases.yaml)

**It does not assign cadet tables to databases**

### Cadet tables
This handles the main bulk of the cadet metadata and depends on Datahub's native [dbt ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/dbt/). 

It creates dataset entities in two platforms within datahub:
1. `dbt` - this is the main Platform, defaulted to by datahub and cannot be changed. These dataset entities are of subtype `Model` or `View`.
2. `Athena` - this is the target platform and is set in the ingestion recipe. These dataset entities are of subtype `Table`.

There is a sibling relationship created for these 2 entities in datahub, but for the purpose of Find MoJ data we only display the entities from the `dbt` platform. This is done by tagging only the `dbt` entities with `dc_display_in_catalogue`. And because users may not be familiar with the terminalogy used in dbt we display `Model` and `View` dataset entities from the `dbt` platform as `Table` in Find MoJ data.

We have also developed a [custom transformers](https://datahubproject.io/docs/actions/guides/developing-a-transformer/) for this ingestion. Transformers are a way of adding metadata to entities during an ingestion see [datahub transfomrer definition](https://datahubproject.io/docs/metadata-ingestion/docs/transformer/intro/) .

These transformers are:
- [assign_cadet_databases](ingestion/transformers/assign_cadet_databases.py) - Assigns each ingested dataset entity to its parent database

The recipe file for this component can be found [here](ingestion/cadet.yaml)

### Cadet ingestion workflow
The workflow for the cadet ingestion can be found [here](.github/workflows/ingest-cadet-metadata.yml)

## Glue databases and tables

These transformers are:
- [enrich_container_transformer](ingestion/transformers/enrich_container_transformer.py) - adds an owner, domain, and tag for a provided container


## Justice Data

## GOV.UK statistical publications 
