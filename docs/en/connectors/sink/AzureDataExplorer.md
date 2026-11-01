# Azure Data Explorer

> Azure Data Explorer (ADX) sink connector

## Support Those Engines

> SeaTunnel Zeta

## Description

The Azure Data Explorer sink connector ingests `SeaTunnelRow` data into ADX using the Kusto ingestion service. It supports queued ingestion for throughput and streaming ingestion for low latency.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| cluster_uri | String | Yes | - | ADX cluster URI, e.g. `https://mycluster.eastus.kusto.windows.net`. |
| database | String | Yes | - | Target database name. |
| table | String | Yes | - | Target table name. |
| client_id | String | Yes | - | Azure AD application (client) ID. |
| client_secret | String | Yes | - | Azure AD application secret. |
| tenant_id | String | Yes | - | Azure AD tenant (directory) ID. |
| ingestion_mapping_reference | String | No | "" | Pre-created ingestion mapping name on the ADX table. |
| ingestion_type | Enum | No | QUEUED | `QUEUED` (default) or `STREAMING`. |
| batch_size | Integer | No | 1000 | Rows to buffer before flushing. |
| flush_interval_ms | Long | No | 30000 | Max milliseconds between flushes regardless of batch size. |

## Task Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
    schema = {
      fields = [
        { name = "id", type = "INT" },
        { name = "name", type = "STRING" },
        { name = "ts", type = "TIMESTAMP" }
      ]
    }
    rows = [
      { fields = [1, "a", "2024-01-01T00:00:00"] }
    ]
  }
}

sink {
  AzureDataExplorer {
    cluster_uri = "https://mycluster.eastus.kusto.windows.net"
    database = "mydb"
    table = "mytable"
    client_id = "${ADX_CLIENT_ID}"
    client_secret = "${ADX_CLIENT_SECRET}"
    tenant_id = "${ADX_TENANT_ID}"
    ingestion_type = "QUEUED"
    batch_size = 1000
    flush_interval_ms = 30000
  }
}
```