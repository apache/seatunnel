# Azure Data Explorer

> Azure Data Explorer (ADX) 写入连接器

## 支持的引擎

> SeaTunnel Zeta

## 描述

Azure Data Explorer 写入连接器通过 Kusto Ingestion 服务将 `SeaTunnelRow` 数据写入 ADX，支持队列写入与流式写入两种方式。

## 关键特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## 写入选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|---|---|---|---|---|
| cluster_uri | String | 是 | - | ADX 集群地址，例如 `https://mycluster.eastus.kusto.windows.net`。 |
| database | String | 是 | - | 目标数据库名称。 |
| table | String | 是 | - | 目标表名。 |
| client_id | String | 是 | - | Azure AD 应用 (client) ID。 |
| client_secret | String | 是 | - | Azure AD 应用密钥。 |
| tenant_id | String | 是 | - | Azure AD 租户 (directory) ID。 |
| ingestion_mapping_reference | String | 否 | "" | 已存在的 ingestion mapping 名称。 |
| ingestion_type | Enum | 否 | QUEUED | `QUEUED`（默认）或 `STREAMING`。 |
| batch_size | Integer | 否 | 1000 | 缓冲后批量写入的行数。 |
| flush_interval_ms | Long | 否 | 30000 | 无论批量大小，最多等待的毫秒数。 |

## 示例

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