import ChangeLog from '../changelog/connector-azurecosmosdb.md';

# AzureCosmosDB

> Azure Cosmos DB source connector

## Description

Read data from Azure Cosmos DB (SQL API) containers.

V1 scope:

- source side of the Azure Cosmos DB SQL API connector
- bounded/batch reads
- schema required
- catalog out of scope
- single split; physical partition/range parallel reads are out of scope

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| uri | string | no | - | Azure Cosmos DB account URI |
| endpoint | string | no | - | Azure Cosmos DB account endpoint |
| key | string | no | - | Azure Cosmos DB account key |
| primary_key | string | no | - | Azure Cosmos DB primary account key |
| secondary_key | string | no | - | Azure Cosmos DB secondary account key |
| primary_connection_string | string | no | - | Azure Cosmos DB primary connection string |
| secondary_connection_string | string | no | - | Azure Cosmos DB secondary connection string |
| database | string | yes | - | Azure Cosmos DB database name |
| container | string | yes | - | Azure Cosmos DB container name |
| schema | config | yes | - | Data schema definition |
| query | string | no | SELECT * FROM c | Cosmos SQL query used to read source data |
| max_item_count | int | no | 100 | Max item count per query page |
| common-options | - | no | - | Source plugin common parameters, see [Source Common Options](../common-options/source-common-options.md) |

`uri` and `endpoint` are treated as aliases. If direct credentials are not provided, the connector can derive the endpoint and key from `primary_connection_string` or `secondary_connection_string`.
`max_item_count` controls the preferred query page size when scanning bounded query results.

### schema [config]

Cosmos DB stores JSON documents and does not enforce your SeaTunnel schema. You should provide `schema.fields` explicitly. For details, refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

Example:

```hocon
schema = {
  fields {
    id = string
    user_id = string
    amount = double
    created_at = timestamp
    labels = "map<string,string>"
  }
}
```

## Task Example

```bash
source {
  AzureCosmosDB {
    uri = "https://example-account.documents.azure.com:443/"
    primary_key = "<cosmos-account-key>"
    database = "app-db"
    container = "orders"
    query = "SELECT c.id, c.user_id, c.amount, c.created_at, c.labels FROM c"
    max_item_count = 200
    schema = {
      fields {
        id = string
        user_id = string
        amount = double
        created_at = timestamp
        labels = "map<string,string>"
      }
    }
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
