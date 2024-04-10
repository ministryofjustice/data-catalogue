This is an early exploration of what a "Data Product" may look like,
as of May 2023.
Migrated from https://github.com/ministryofjustice/data-platform-products

# Data products

## Purpose

A data product exists to help with interoperability, to make that process simpler. The intention of data products and the resulting data mesh is to eliminate the struggle to get timely access to data, and a consequent loss of trust. The purpose of the data product is to serve the consumer's needs.

A Data Product is created and owned by a Data Product Owner, a person with comprehensive domain knowledge. The Data Product Owner is not part of the Data Platform team, they are people that operate on different teams and they have a system that contains data that they would like to share. This is because it is essential to know a domain before creating a Data Product, and it would be impossible for the Data Platform teams to have deep knowledge of every domain that uses the platform.

## Goals

Our goals are:

- Make data easily discoverable by users who wish to use that data. We do this by encouraging the owners of data products to supply high quality [metadata](https://en.wikipedia.org/wiki/Metadata)
- Make data more usable, whatever the purpose, by applying product thinking to our data, and clearly describing the lineage and transformations of our data products
- Make it easier for us to provide governance for data, for example by identfying owners, protective markings and retention periods.

## Defining a data product

A data product will have a unique name, and is defined using a collection of YAML files.

| File name                | Purpose                                                                                                                                                                       | Documentation                                                                                       |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `00-specification.yml`   | Aids data discoverability by providing a name, description and tags for a data product. It also contains contact details of the data product owner.                           | [Data product specification](./_docs/product-specification.md)                                      |
| `01-governance.yml`      | Contains protective marking, retention period, expected update frequency and data lineage.                                                                                    | [Defining product governance](./_docs/product-governance.md)                                        |
| `02-data-dictionary.yml` | Contains the field and column (or domain, attribute and value) defintions comprising the data product, along with type information and user-friendly names.                   | [Data dictionary guidance](./_docs/data-dictionary.md)                                              |
| `03-transformations.yml` | Describes the [cleaning](./_docs/cleansing-definitions.md) and [transformation](./_docs/transform-definitions.md) data will undergo before it is made available to consumers. | [Cleaning](./_docs/cleansing-definitions.md) and [transformation](./_docs/transform-definitions.md) |
