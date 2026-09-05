import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus 源连接器

## 描述

Milvus 源连接器用于从 Milvus 或 Zilliz Cloud 读取数据。它可以读取一个集合，
也可以读取某个数据库下的所有集合，并且会把 Milvus 的分区信息、向量索引信息等元数据传给下游，
下游连接器支持时可以继续使用这些信息。

常见用法：

- 配置 `collection` 读取一个 Milvus 集合。
- 不配置 `collection` 时，读取指定数据库下的所有集合。
- 从 Milvus 复制到 Milvus，并保留向量字段、分区元数据和索引元数据。
- 读取 `FLOAT_VECTOR`、`BINARY_VECTOR`、`FLOAT16_VECTOR`、`BFLOAT16_VECTOR` 和 `SPARSE_FLOAT_VECTOR` 字段。
- 遇到限流或 gRPC 限制时自动重试。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行读取](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

|  Milvus 数据类型   | SeaTunnel 数据类型 |
|---------------------|---------------------|
| INT8                | TINYINT             |
| INT16               | SMALLINT            |
| INT32               | INT                 |
| INT64               | BIGINT              |
| FLOAT               | FLOAT               |
| DOUBLE              | DOUBLE              |
| BOOL                | BOOLEAN             |
| JSON                | STRING              |
| ARRAY               | ARRAY               |
| VARCHAR             | STRING              |
| FLOAT_VECTOR        | FLOAT_VECTOR        |
| BINARY_VECTOR       | BINARY_VECTOR       |
| FLOAT16_VECTOR      | FLOAT16_VECTOR      |
| BFLOAT16_VECTOR     | BFLOAT16_VECTOR     |
| SPARSE_FLOAT_VECTOR | SPARSE_FLOAT_VECTOR |

## 源选项

| 名称         | 类型     | 是否必传 | 默认值     | 描述                                                                                         |
|------------|--------|------|---------|--------------------------------------------------------------------------------------------|
| url        | String | 是    | -       | Milvus 或 Zilliz Cloud 的连接地址，例如 `http://127.0.0.1:19530`。                                 |
| token      | String | 是    | -       | Milvus 认证令牌。本地 Milvus 通常使用 `username:password`。                                            |
| database   | String | 否    | default | 源数据库。                                                                                      |
| collection | String | 否    | -       | 源集合。配置后只读取这个集合；不配置时读取 `database` 下的所有集合。旧别名 `collection_name` 也仍然支持。                |
| batch_size | Integer | 否 | 1000    | 每次从 Milvus 拉取的记录数。值越大吞吐越高，但内存占用也越大；记录中包含较大向量负载时可以适当调小。                                       |
| rate_limit | Integer | 否 | 1000000 | Source 每秒最多向 Milvus 请求的记录数。用于在共享 Milvus 配额（QPS）或 gRPC 消息大小限制下对流任务进行限速。设为 `-1` 关闭限速。       |

## 注意事项

- `database` 默认是 `default`，本地 Milvus 的简单任务通常不用配置。
- `collection` 是可选项。只想读一个集合时再配置。
- `batch_size` 控制单次拉取的页面大小，与 reader 的并行度无关。需要配合 `parallelism` 一起调整，以平衡吞吐和内存。
- `rate_limit` 是 Milvus 服务端的提示，用于在大批量向量读取时规避 `GRPC limit` 错误。除非日志里出现限速或 gRPC 报错，否则保持默认值即可。
- 不配置 `collection` 时，源端会发现 `database` 下的所有集合，并把每个集合作为一张独立的 SeaTunnel 表输出。
- 源端会按 Milvus 分区拆分读取任务。有分区键的集合使用一个 split 读取；没有分区键的集合会按分区名拆分，并分配给多个 reader。
- 源端读取带分区的集合时，下游 Milvus 接收器可以利用这些元数据在目标集合创建相同分区名。
- 源端读取到向量索引信息时，下游 Milvus 接收器可以配合 `create_index = true` 创建相同向量索引。
- Milvus 源是 BOUNDED（有界）源：作业在所有分区（split）扫描完成后会自然结束，不会像 Kafka、Fluss 那样提供按记录级别 offset 持续增量读取。检查点/恢复以 split（分区）为粒度——已经扫描完成的分区不会被重读，但作业失败时正在扫描的分区会从分区开头重新扫描；如果希望持续摄入新增向量，需要在外部（例如业务写入端）配合周期性重新提交 SeaTunnel 作业。

## 任务示例

### 读取一个集合

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
    collection = "simple_example"
  }
}

sink {
  Console {}
}
```

### 读取一个数据库下的所有集合

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
  }
}

sink {
  Console {}
}
```

### 复制一个 Milvus 集合到另一个数据库

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    collection = "simple_example"
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "test"
    collection = "simple_example"
  }
}
```

### 复制集合并重建向量索引

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    collection = "simple_example"
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "test_index_preservation"
    collection = "simple_example_preservation"
    create_index = true
  }
}
```

### 配合检查点周期性地重跑

Milvus 源是 BOUNDED 的，所有分区扫描完成后作业会自然结束。本示例以
`STREAMING` 模式运行，并设置较短的检查点间隔——如果希望持续摄入新增向量，
可以让外部调度器按需重新提交作业；恢复时以 split（分区）为粒度，已经完整扫描
的分区不会被重读，正在扫描时失败的分区会从分区开头重新读取。下游 Sink 设置
`enable_upsert = true`，配合主键去重避免重复写入。

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "streaming_test"
    collection = "simple_example"
    batch_size = 500
    rate_limit = 200000
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "streaming_test"
    enable_upsert = true
    batch_size = 1000
  }
}
```

### 限制读取速度以保护共享集群

当 Milvus 集群同时被其他任务共享时，可以调小 `rate_limit` 和 `batch_size`，
避免 Source 超过集群的 gRPC 消息大小限制。

```bash
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "shared"
    batch_size = 200
    rate_limit = 100000
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
