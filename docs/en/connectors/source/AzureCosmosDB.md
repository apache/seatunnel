import ChangeLog from '../changelog/connector-azurecosmosdb.md';

# AzureCosmosDB

> Azure Cosmos DB source connector

## Support Connector Version

- Azure Cosmos DB SQL (Core) API accounts

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

Read data from Azure Cosmos DB (SQL API) containers using bounded batch scans.

V1 scope for this connector:

- source only
- bounded/batch reads with a Cosmos SQL query
- schema required (no schema inference)
- catalog discovery out of scope
- single split reading; physical partition/range parallel reads are out of scope
- change feed, managed identity, and container creation are out of scope

## Supported DataSource Info

In order to use the AzureCosmosDB connector, the following dependency is required.
It can be installed via `install-plugin.sh` or downloaded from the Maven Central Repository.

| Datasource     | Supported Versions              | Dependency                                                                                         |
|----------------|---------------------------------|----------------------------------------------------------------------------------------------------|
| AzureCosmosDB  | SQL (Core) API                  | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-azurecosmosdb)      |

## Database Dependency

> Please install the connector plugin before running jobs:

```shell
sh bin/install-plugin.sh ${version}
```

Ensure `connector-azurecosmosdb` is included in your plugin installation. The connector uses the Azure Cosmos Java SDK (`azure-cosmos` 4.63.0) at runtime.

## Data Type Mapping

Cosmos DB stores JSON documents. You must define the SeaTunnel schema explicitly. The connector maps JSON values to SeaTunnel types according to your configured field types:

| Cosmos JSON value | SeaTunnel Data type (when configured) |
|-------------------|---------------------------------------|
| Boolean           | BOOLEAN                               |
| Number            | TINYINT / SMALLINT / INT / BIGINT / FLOAT / DOUBLE |
| String            | STRING                                |
| String (ISO-8601) | DATE / TIME / TIMESTAMP               |
| String (decimal)  | DECIMAL                               |
| Binary            | BYTES                                 |
| Object            | MAP / ROW                             |
| Array             | ARRAY                                 |
| Null              | null                                  |

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

### uri [string]

Azure Cosmos DB account URI. Treated as an alias of `endpoint`.

### endpoint [string]

Azure Cosmos DB account endpoint, for example `https://example-account.documents.azure.com:443/`.

### key [string]

Azure Cosmos DB account key.

### primary_key [string]

Azure Cosmos DB primary account key.

### secondary_key [string]

Azure Cosmos DB secondary account key.

### primary_connection_string [string]

Azure Cosmos DB primary connection string. The connector can parse `AccountEndpoint` and `AccountKey` from it.

### secondary_connection_string [string]

Azure Cosmos DB secondary connection string. The connector can parse `AccountEndpoint` and `AccountKey` from it.

### database [string]

Target database name. The database must already exist.

### container [string]

Target container name. The container must already exist.

### schema [config]

Cosmos DB stores JSON documents and does not enforce your SeaTunnel schema. You must provide `schema.fields` explicitly. For details, refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

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

### query [string]

Cosmos SQL query used for bounded batch reads. Use this for filtering and field projection, for example `SELECT c.id, c.name FROM c WHERE c.score > 10`.

### max_item_count [int]

Preferred page size when the SDK iterates query results. During checkpoint restore, the connector persists the SDK continuation token for the in-flight paginated query and resumes from the last completed page.

### common-options

Source plugin common parameters, refer to [Source Common Options](../common-options/source-common-options.md) for details.

### Tips

> 1. You must provide at least one of `uri`, `endpoint`, `primary_connection_string`, or `secondary_connection_string`, and at least one of `key`, `primary_key`, `secondary_key`, or a connection string that contains an account key.<br/>
> 2. V1 uses a single split. Increasing source parallelism does not parallelize Cosmos reads across physical partitions.<br/>
> 3. Use Cosmos SQL in `query` for filtering and projection. Connector-level column projection is not supported as a separate feature.<br/>
> 4. The connector reads from an existing container only. It does not create databases, containers, or indexes.
> 5. Checkpoint resume is based on Cosmos query page boundaries, not individual rows. Change feed reading is still out of scope.

## How to Create an Azure Cosmos DB Data Synchronization Job

The following example demonstrates how to create a batch job that reads data from Azure Cosmos DB and prints it to the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

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
  Console {
    parallelism = 1
  }
}
```

### Query with filter and smaller page size

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AzureCosmosDB {
    endpoint = "https://example-account.documents.azure.com:443/"
    key = "<cosmos-account-key>"
    database = "app-db"
    container = "orders"
    query = "SELECT c.id, c.user_id, c.amount FROM c WHERE c.amount > 100"
    max_item_count = 50
    schema = {
      fields {
        id = string
        user_id = string
        amount = double
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
