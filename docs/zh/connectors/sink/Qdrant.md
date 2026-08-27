import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant 数据写入连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

[Qdrant](https://qdrant.tech/) 是一个高性能的向量搜索引擎和向量数据库。

Qdrant sink 会把 SeaTunnel 行写入一个已经存在的 Qdrant collection。普通列会写入 point payload，向量列会写成命名向量；如果上游 schema 中有主键列，主键值会作为 Qdrant point ID。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称            | 类型   | 必填 | 默认值    | 说明 |
|-----------------|--------|------|-----------|------|
| collection_name | string | 是   | -         | 要写入的 Qdrant collection 名称。普通任务中请显式配置该项。 |
| host            | string | 否   | localhost | Qdrant gRPC 主机。 |
| port            | int    | 否   | 6334      | Qdrant gRPC 端口。 |
| api_key         | string | 否   | ""        | 认证场景下使用的 Qdrant API key。 |
| use_tls         | bool   | 否   | false     | gRPC 连接是否启用 TLS。 |
| common-options  |        | 否   | -         | Sink 通用选项。 |

### collection_name [string]

要写入的 Qdrant collection 名称。目标 collection 必须已经存在，并且向量名、向量维度要和 SeaTunnel 向量列一致。

### host [string]

Qdrant 实例的主机名。

### port [int]

Qdrant 实例的 gRPC 端口。

### api_key [string]

连接需要认证的 Qdrant 部署时使用的 API key。如果 Qdrant 服务不需要认证，可以保持为空。

### use_tls [bool]

gRPC 连接是否启用 TLS。连接 Qdrant Cloud 或其他 HTTPS/TLS 地址时通常需要开启。

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

## 支持的数据类型

| SeaTunnel 类型 | Qdrant 值 |
|----------------|-----------|
| SMALLINT       | payload integer |
| INT            | payload integer 或数字 point ID |
| BIGINT         | payload integer |
| FLOAT          | payload double |
| DOUBLE         | payload double |
| STRING         | payload 字符串或 UUID point ID |
| DATE           | payload 字符串 |
| BOOLEAN        | payload bool |
| FLOAT_VECTOR   | 命名向量 |
| BINARY_VECTOR  | 命名向量 |
| FLOAT16_VECTOR | 命名向量 |
| BFLOAT16_VECTOR | 命名向量 |

主键列的值会作为 Qdrant point ID。主键值必须是 `INT` 数字 ID 或 `STRING` UUID。如果没有主键，sink 会为每行生成一个随机 UUID。

## 注意事项

- 目标 collection 必须在作业启动前已经存在。连接器不会自动创建 collection 或向量索引。
- 向量列名和维度必须与目标 Qdrant collection 的向量配置一致。
- sink 会把向量列写成命名向量。目标 collection 中的命名向量需要和 SeaTunnel 向量列名一致。
- 写入 Qdrant 前，请先处理 payload 和向量字段中的空值，因为写入器会把每个配置字段直接转换成 Qdrant point 值。
- sink 会把每条输入行作为 upsert 写入，不会按 `UPDATE` 或 `DELETE` 行类型执行 CDC 语义。
- 写入器最多缓存 64 个 point，并且在关闭写入器或准备提交时也会刷新。这一批次大小目前不是可配置项。
- 每个 sink 配置块写入一个 collection。如果不同 collection 需要不同配置，请使用多个 sink 配置块。

## 任务示例

下面的示例会把一个 Qdrant collection 中的记录写入另一个 Qdrant collection。`file_name` 和 `file_size` 会写成 point payload 字段，`my_vector` 会写成命名向量。

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
