import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant 数据源连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

[Qdrant](https://qdrant.tech/) 是一个高性能的向量搜索引擎和向量数据库。

Qdrant source 用来从一个已经存在的 Qdrant collection 读取 point。point 的 payload 字段会读成普通 SeaTunnel 列，Qdrant 向量会读成 SeaTunnel 向量列。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行读取](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称            | 类型   | 必填 | 默认值    | 说明 |
|-----------------|--------|------|-----------|------|
| collection_name | string | 是   | -         | 要读取的 Qdrant collection 名称。 |
| schema          | config | 是   | -         | SeaTunnel schema，用来映射 Qdrant point ID、payload 字段和向量。 |
| host            | string | 否   | localhost | Qdrant gRPC 主机。 |
| port            | int    | 否   | 6334      | Qdrant gRPC 端口。 |
| api_key         | string | 否   | ""        | 认证场景下使用的 Qdrant API key。 |
| use_tls         | bool   | 否   | false     | gRPC 连接是否启用 TLS。 |
| common-options  |        | 否   | -         | Source 通用选项。 |

### collection_name [string]

要读取的 Qdrant collection 名称。

### schema [config]

source 输出的 SeaTunnel 行结构。更多说明请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

Qdrant 中的每条记录叫作 point：

- SeaTunnel 主键列会从 Qdrant point ID 中读取。Qdrant point ID 可以是正整数或 UUID 字符串。
- 向量列会从 Qdrant 向量中读取。读取命名向量时，SeaTunnel 列名必须和 Qdrant 向量名一致。
- 其他支持的列会从 Qdrant point payload 中读取。
- 如果 collection 使用默认的未命名向量，请使用 `default_vector` 作为 SeaTunnel 向量列名。
- schema 中只配置实际存在的 payload 或向量字段。Source 会按字段名直接取值，配置的字段需要在读取到的 point 中存在。

示例：

```hocon
schema = {
  columns = [
    {
      name = file_name
      type = string
    }
    {
      name = file_size
      type = int
    }
    {
      name = my_vector
      type = float_vector
    }
  ]
}
```

### host [string]

Qdrant 实例的主机名。

### port [int]

Qdrant 实例的 gRPC 端口。

### api_key [string]

连接需要认证的 Qdrant 部署时使用的 API key。如果 Qdrant 服务不需要认证，可以保持为空。

### use_tls [bool]

gRPC 连接是否启用 TLS。连接 Qdrant Cloud 或其他 HTTPS/TLS 地址时通常需要开启。

### 通用选项

Source 插件通用参数，请参考 [Source 通用选项](../common-options/source-common-options.md)。

## 支持的数据类型

| SeaTunnel 类型 | Qdrant 值 |
|----------------|-----------|
| STRING         | payload 字符串或 point UUID |
| BOOLEAN        | payload bool |
| TINYINT        | payload integer |
| SMALLINT       | payload integer |
| INT            | payload integer 或 point 数字 ID |
| BIGINT         | payload integer |
| FLOAT          | payload double |
| DOUBLE         | payload double |
| DECIMAL        | payload double |
| FLOAT_VECTOR   | vector |
| BINARY_VECTOR  | vector |
| FLOAT16_VECTOR | vector |
| BFLOAT16_VECTOR | vector |

## 注意事项

- 作业启动前，collection 必须已经存在。
- Qdrant 中的向量字段名和维度必须与 SeaTunnel schema 中的向量列一致。
- 如果 collection 只有一个未命名向量，请把 SeaTunnel 向量列命名为 `default_vector`。
- Qdrant source 是有界读取，并且只使用一个 split，不支持并行读取。
- Qdrant source 只输出 `INSERT` 行，不读取 CDC 变更。

## 任务示例

下面的示例从 `source_collection` 读取 payload 字段和命名向量，然后写入 `sink_collection`。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Qdrant {
    collection_name = "source_collection"
    host = "localhost"
    port = 6334
    schema = {
      columns = [
        {
          name = file_name
          type = string
        }
        {
          name = file_size
          type = int
        }
        {
          name = my_vector
          type = float_vector
        }
      ]
    }
  }
}

sink {
  Qdrant {
    collection_name = "sink_collection"
    host = "localhost"
    port = 6334
  }
}
```

## 变更日志

<ChangeLog />
