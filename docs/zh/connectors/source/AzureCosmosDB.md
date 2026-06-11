import ChangeLog from '../changelog/connector-azurecosmosdb.md';

# AzureCosmosDB

> Azure Cosmos DB 源连接器

## 描述

从 Azure Cosmos DB（SQL API）容器读取数据。

V1 范围：

- Azure Cosmos DB SQL API 连接器的 source 部分
- 仅 bounded/batch 读取
- 必须显式配置 schema
- catalog 暂不支持
- 单分片读取；物理分区/范围并行读取暂不支持

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 名称 | 类型 | 必需 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| uri | string | 否 | - | Azure Cosmos DB 账号 URI |
| endpoint | string | 否 | - | Azure Cosmos DB 账号 endpoint |
| key | string | 否 | - | Azure Cosmos DB 账号 key |
| primary_key | string | 否 | - | Azure Cosmos DB 主账号 key |
| secondary_key | string | 否 | - | Azure Cosmos DB 次账号 key |
| primary_connection_string | string | 否 | - | Azure Cosmos DB 主连接字符串 |
| secondary_connection_string | string | 否 | - | Azure Cosmos DB 次连接字符串 |
| database | string | 是 | - | Azure Cosmos DB 数据库名 |
| container | string | 是 | - | Azure Cosmos DB 容器名 |
| schema | config | 是 | - | 数据 schema 定义 |
| query | string | 否 | SELECT * FROM c | 读取数据使用的 Cosmos SQL |
| max_item_count | int | 否 | 100 | 每页查询最大记录数 |
| common-options | - | 否 | - | 源插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md) |

`uri` 和 `endpoint` 视为同义配置。如果没有直接提供密钥，连接器可以从 `primary_connection_string` 或 `secondary_connection_string` 中解析出 endpoint 和 key。
`max_item_count` 用于控制有界查询扫描时的首选分页大小。

### schema [config]

Cosmos DB 存储 JSON 文档，不会自动推断 SeaTunnel schema。建议显式配置 `schema.fields`。更多信息请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

示例：

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

## 任务示例

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

## 变更日志

<ChangeLog />
