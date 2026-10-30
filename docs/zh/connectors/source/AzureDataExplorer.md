# Azure Data Explorer

> Azure Data Explorer (ADX) 源连接器

## 支持的引擎

> SeaTunnel Zeta

## 描述

Azure Data Explorer 源连接器用于执行 Kusto Query Language (KQL) 查询，并将结果以 `SeaTunnelRow` 输出。

## 关键特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|---|---|---|---|---|
| cluster_uri | String | 是 | - | ADX 集群地址，例如 `https://mycluster.eastus.kusto.windows.net`。 |
| database | String | 是 | - | 目标数据库名称。 |
| query | String | 是 | - | 要执行的 KQL 查询。 |
| client_id | String | 是 | - | Azure AD 应用 (client) ID。 |
| client_secret | String | 是 | - | Azure AD 应用密钥。 |
| tenant_id | String | 是 | - | Azure AD 租户 (directory) ID。 |
| schema | Config | 否 | - | 可选的 SeaTunnel schema。参考 [Source Common Options](../common-options/source-common-options.md)。 |

## 示例

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