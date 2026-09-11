import ChangeLog from '../changelog/connector-azurecosmosdb.md';

# AzureCosmosDB

> Azure Cosmos DB 源连接器

## 连接器支持版本

- Azure Cosmos DB SQL（Core）API 账户

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 描述

通过有界批处理扫描，从 Azure Cosmos DB（SQL API）容器读取数据。

本连接器 V1 范围：

- 仅 source
- 使用 Cosmos SQL 的有界/批处理读取
- 必须显式配置 schema（不支持 schema 推断）
- 不支持 catalog 发现
- 单分片读取；物理分区/范围并行读取暂不支持
- change feed、托管身份认证、容器创建均不在 V1 范围内

## 支持的数据源信息

使用 AzureCosmosDB 连接器需要以下依赖。
可通过 `install-plugin.sh` 安装，或从 Maven Central 下载。

| 数据源         | 支持的版本           | 依赖                                                                                         |
|----------------|----------------------|----------------------------------------------------------------------------------------------|
| AzureCosmosDB  | SQL（Core）API       | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-azurecosmosdb) |

## 数据库依赖

> 运行作业前请先安装连接器插件：

```shell
sh bin/install-plugin.sh ${version}
```

请确保安装 `connector-azurecosmosdb` 插件。连接器运行时使用 Azure Cosmos Java SDK（`azure-cosmos` 4.63.0）。

## 数据类型映射

Cosmos DB 存储 JSON 文档。你必须显式定义 SeaTunnel schema。连接器会根据你配置的字段类型，将 JSON 值映射为 SeaTunnel 类型：

| Cosmos JSON 值 | SeaTunnel 数据类型（按 schema 配置） |
|----------------|--------------------------------------|
| Boolean        | BOOLEAN                              |
| Number         | TINYINT / SMALLINT / INT / BIGINT / FLOAT / DOUBLE |
| String         | STRING                               |
| String (ISO-8601) | DATE / TIME / TIMESTAMP           |
| String (decimal)  | DECIMAL                           |
| Binary         | BYTES                                |
| Object         | MAP / ROW                            |
| Array          | ARRAY                                |
| Null           | null                                 |

## 源配置项

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

### uri [string]

Azure Cosmos DB 账号 URI，与 `endpoint` 视为同义配置。

### endpoint [string]

Azure Cosmos DB 账号 endpoint，例如 `https://example-account.documents.azure.com:443/`。

### key [string]

Azure Cosmos DB 账号 key。

### primary_key [string]

Azure Cosmos DB 主账号 key。

### secondary_key [string]

Azure Cosmos DB 次账号 key。

### primary_connection_string [string]

Azure Cosmos DB 主连接字符串。连接器可从中解析 `AccountEndpoint` 与 `AccountKey`。

### secondary_connection_string [string]

Azure Cosmos DB 次连接字符串。连接器可从中解析 `AccountEndpoint` 与 `AccountKey`。

### database [string]

目标数据库名。数据库必须已存在。

### container [string]

目标容器名。容器必须已存在。

### schema [config]

Cosmos DB 存储 JSON 文档，不会自动推断 SeaTunnel schema。必须显式配置 `schema.fields`。更多信息请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

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

### query [string]

用于有界批处理读取的 Cosmos SQL。可用于过滤和字段投影，例如 `SELECT c.id, c.name FROM c WHERE c.score > 10`。

### max_item_count [int]

SDK 迭代查询结果时的首选分页大小。执行 checkpoint 恢复时，连接器会持久化当前分页查询的 SDK continuation token，并从上一个完成的分页继续读取。

### common-options

源插件通用参数，请参考 [Source Common Options](../common-options/source-common-options.md)。

### 提示

> 1. 必须至少提供 `uri`、`endpoint`、`primary_connection_string`、`secondary_connection_string` 之一，以及 `key`、`primary_key`、`secondary_key` 之一，或包含 account key 的连接字符串。<br/>
> 2. V1 仅使用单个 split。提高 source 并行度不会按物理分区并行读取 Cosmos 数据。<br/>
> 3. 可在 `query` 中使用 Cosmos SQL 做过滤和投影。连接器不提供单独的列投影特性。<br/>
> 4. 连接器只读取已有容器，不会创建数据库、容器或索引。
> 5. Checkpoint 恢复基于 Cosmos 查询分页边界，而不是单行边界。Change feed 读取仍不在范围内。

## 如何创建 Azure Cosmos DB 数据同步作业

以下示例演示如何创建批处理作业，从 Azure Cosmos DB 读取数据并打印到本地客户端：

```bash
# 设置要执行的任务的基本配置
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

### 带过滤条件与较小分页大小的查询

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

## 变更日志

<ChangeLog />
