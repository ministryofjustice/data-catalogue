# Data catalogue

This is the root repository for the MOJ data catalogue.

## Component repositories

- [find-moj-data](https://github.com/ministryofjustice/find-moj-data/) - a read only view of the catalogue with GOV.UK styles
- [data-catalogue-metadata](https://github.com/ministryofjustice/data-catalogue-metadata) (internal) contains JSON files used to populate the catalogue in development

## Environments
- [Datahub (dev)](https://datahub-catalogue-dev.apps.live.cloud-platform.service.justice.gov.uk/)
- [Find MOJ data (dev)](https://data-platform-find-moj-data-dev.apps.live.cloud-platform.service.justice.gov.uk/)

## Datahub (Backend)

### Administration via command line

#### First time setup

Run `datahub init` and provide the following credentials

- server: https://data-platform-datahub-catalogue-dev.apps.live.cloud-platform.service.justice.gov.uk
- token: `<generate PAT via the UI>`

You may also need to set the environment variable `export DATAHUB_GMS_URL="https://data-platform-datahub-catalogue-dev.apps.live.cloud-platform.service.justice.gov.uk/api/gms"`

#### Import metadata into a Datahub lite

[Datahub lite](https://datahubproject.io/docs/datahub_lite/) is a developer interface for local debugging.

lite_sink.yaml:

```yaml
pipeline_name: datahub_source_1
datahub_api:
  server: "https://data-platform-datahub-catalogue-dev.apps.live.cloud-platform.service.justice.gov.uk/api/gms" 
  token: "xxxxx"
source:
  type: datahub
  config:
    include_all_versions: false
    pull_from_datahub_api: true
sink:
  type: datahub-lite
```

```
datahub ingest -c lite_sink.yaml

datahub lite ls
```
