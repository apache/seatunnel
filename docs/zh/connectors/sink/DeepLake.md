import ChangeLog from '../changelog/connector-deeplake.md';

# DeepLake

> Deep Lake Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

DeepLake Sink 将 SeaTunnel 数据追加写入 Deep Lake 托管服务中的表。连接器使用 Deep Lake REST SQL API，SeaTunnel Worker 不需要安装 Python 或原生 Deep Lake 客户端。

当前仅支持可通过 REST API 访问的托管 Workspace，不支持本地 Deep Lake 数据集或仅限 Python 的存储路径。

该连接器只接受追加数据。遇到 `UPDATE_BEFORE`、`UPDATE_AFTER` 或 `DELETE` 时会直接失败，避免将 CDC 记录错误地追加到目标表。

## Sink 参数

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| api_url | string | 否 | `https://api.deeplake.ai` | Deep Lake REST API 地址。 |
| api_key | string | 是 | - | Deep Lake API Key，请勿提交到源码仓库。 |
| org_id | string | 是 | - | 每个请求携带的 Activeloop 组织 ID。 |
| workspace | string | 是 | - | 目标表所在的 Workspace。 |
| table | string | 否 | 上游表名 | 目标表名。 |
| batch_size | int | 否 | `100` | 单次批量请求最多写入的行数。 |
| connect_timeout_ms | int | 否 | `10000` | HTTP 连接超时时间，单位为毫秒。 |
| socket_timeout_ms | int | 否 | `60000` | HTTP Socket 超时时间，单位为毫秒。 |
| schema_save_mode | enum | 否 | `CREATE_SCHEMA_WHEN_NOT_EXIST` | Schema 处理方式。支持 `CREATE_SCHEMA_WHEN_NOT_EXIST`、`ERROR_WHEN_SCHEMA_NOT_EXIST` 和 `IGNORE`。 |
| multi_table_sink_replica | int | 否 | `1` | 多表写入的并行副本数。 |

### schema_save_mode

- `CREATE_SCHEMA_WHEN_NOT_EXIST`：写入前执行 `CREATE TABLE IF NOT EXISTS ... USING deeplake`。
- `ERROR_WHEN_SCHEMA_NOT_EXIST`：使用空查询验证目标表，不存在时失败。
- `IGNORE`：假设目标表已存在并跳过 Schema 验证。

不支持 `RECREATE_SCHEMA`，因为删除托管数据集具有破坏性，并且多个 Sink Writer 并发启动时不安全。

## 数据类型映射

| SeaTunnel 数据类型 | Deep Lake SQL 类型 |
|-------------------|--------------------|
| BOOLEAN | BOOLEAN |
| TINYINT | SMALLINT |
| SMALLINT | SMALLINT |
| INT | INTEGER |
| BIGINT | BIGINT |
| FLOAT | REAL |
| DOUBLE | DOUBLE PRECISION |
| DECIMAL | NUMERIC(precision, scale) |
| STRING | TEXT |
| BYTES | BYTEA |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| TIMESTAMP_TZ | TIMESTAMPTZ |
| FLOAT_VECTOR | FLOAT4[] |
| BINARY_VECTOR | BYTEA |
| ARRAY | 对应受支持 Deep Lake 元素类型的数组 |

首个版本不支持 `FLOAT16_VECTOR`、`BFLOAT16_VECTOR`、`SPARSE_FLOAT_VECTOR`、`MAP`、`ROW`，也不支持包含 `BYTES` 或 `BINARY_VECTOR` 的数组。SQL 生成阶段会直接失败，不会以精度或结构损失的方式转换这些数据。

## 交付语义

数据先缓存在内存中，再通过 Deep Lake 参数化批量查询接口写入。只有 HTTP 请求成功后才清空缓存；达到 `batch_size`、准备 Checkpoint 和关闭正常 Writer 时都会刷新缓存。写入失败后 Writer 会进入终止状态，关闭时不会重试结果不明确的批次。

连接器提供至少一次交付。Deep Lake 已接受请求但 SeaTunnel 尚未记录 Checkpoint 时，如果任务失败，恢复后可能再次发送同一批数据。稳定的主键可以发现重复数据，但不能让重试变成精确一次语义。无法接受重复数据时，应在写入前或写入后去重。

连接器按同名列映射每个输入字段，不会生成合成记录 ID。目标表需要稳定的业务标识时，应在上游 schema 中定义主键。

## 任务示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        document_id = bigint
        content = string
        score = double
      }
    }
    plugin_output = "documents"
  }
}

sink {
  DeepLake {
    plugin_input = "documents"
    api_key = "${DEEPLAKE_API_KEY}"
    org_id = "${DEEPLAKE_ORG_ID}"
    workspace = "research"
    table = "documents"
    batch_size = 100
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  }
}
```

## 更新日志

<ChangeLog />
