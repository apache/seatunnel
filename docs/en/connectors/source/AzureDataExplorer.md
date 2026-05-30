# Azure Data Explorer

> Azure Data Explorer (ADX) source connector

## Support Those Engines

> SeaTunnel Zeta

## Description

The Azure Data Explorer source connector executes a Kusto Query Language (KQL) statement against an ADX cluster and emits the query results as `SeaTunnelRow` records.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| cluster_uri | String | Yes | - | ADX cluster URI, e.g. `https://mycluster.eastus.kusto.windows.net`. |
| database | String | Yes | - | Target database name. |
| query | String | Yes | - | Kusto query (KQL) to execute. |
| client_id | String | Yes | - | Azure AD application (client) ID. |
| client_secret | String | Yes | - | Azure AD application secret. |
| tenant_id | String | Yes | - | Azure AD tenant (directory) ID. |
| schema | Config | No | - | Optional SeaTunnel schema. See [Source Common Options](../common-options/source-common-options.md). |

## Task Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AzureDataExplorer {
    cluster_uri = "https://mycluster.eastus.kusto.windows.net"
    database = "mydb"
    query = "MyTable | take 1000"
    client_id = "${ADX_CLIENT_ID}"
    client_secret = "${ADX_CLIENT_SECRET}"
    tenant_id = "${ADX_TENANT_ID}"

    schema = {
      fields = [
        { name = "id", type = "INT" },
        { name = "name", type = "STRING" },
        { name = "ts", type = "TIMESTAMP" }
      ]
    }
  }
}

sink {
  Console {
  }
}
```