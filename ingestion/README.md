# Datahub metadata ingestions
Here is where we have the configuration files ([recipes](https://datahubproject.io/docs/metadata-ingestion/recipe_overview/)) for the majority of our ingestions into Datahub on a daily schedule and also the code we've written for our custom ingestion sources, those that are specific to us and not natively supported by Datahub.

We have some other metadata in [data-catalogue-metadata](https://github.com/ministryofjustice/data-catalogue-metadata), however this is an internal repositorty and has currently been used for ingestions we've not scheduled and do not have as robust a way to automate the ingestion pipeline.


## Create a Derived Table (Cadet) ingestion

This uses Datahub's native [dbt ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/dbt/) although we have had to supplement this with some custom code to create container (database) entities in datahub and assign the ingested dbt models to these databases.

The cadet ingestion has the following components

### Create cadet databases
A custom ingestion [source](ingestion/create_cadet_databases_source/source) and [config](ingestion/create_cadet_databases_source/config) to create databases for every dbt model entity we ingest that has been tagged `dc_display_in_catalogue`.

This is required because the dbt manifest file, used to ingest the metadata from, does not contain any metadata at the database level and the Datahub ingestion only ingests at the model level (which can be considered equivalent to a database table)  

In the simplest terms this ingestion infers parent databases using the entities within the manifest and creates the container entities of subtype databases within datahub
