import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable Source 连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>

## 描述

使用原生 Bigtable Data v2 Java 客户端从 Google Cloud Bigtable 读取数据。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

:::tip

该 Source 是有界读取。Enumerator 会调用 Bigtable `sampleRowKeys`，把整表（或配置的 `start_rowkey` / `end_rowkey` 范围）按 tablet 切成多个 split，再按 `hash(splitId) % parallelism` 分配。将 `env.parallelism`（或 Source 并行度）设为大于 1，多个 Reader 会扫描不同 key range。若采样失败、返回空，或与用户范围求交后没有任何有效区间，则回退为单个 split，作业仍可运行。每次扫描会读取所请求行范围内的全部 Cell，并为每个 Bigtable 行输出一条 SeaTunnel 记录。

:::

## 参数

| 参数名            | 类型   | 是否必填 | 默认值 |
|-----------------|--------|--------|------|
| project_id      | string | 是     | -    |
| instance_id     | string | 是     | -    |
| table           | string | 是     | -    |
| credentials_path| string | 否     | -    |
| rowkey_column   | list   | 否     | -    |
| start_rowkey    | string | 否     | -    |
| end_rowkey      | string | 否     | -    |
| start_timestamp | long   | 否     | -    |
| end_timestamp   | long   | 否     | -    |
| max_versions    | int    | 否     | 1    |
| scan_row_limit  | int    | 否     | -1   |
| common-options  |        | 否     | -    |

### project_id [string]

Google Cloud 项目 ID。

### instance_id [string]

Bigtable 实例 ID。

### table [string]

要读取的 Bigtable 表名。

### credentials_path [string]

Google Cloud 服务账号 JSON 密钥文件路径。未设置时使用应用默认凭证（ADC）。在 GCE/GKE 节点上、`gcloud` shell 会话中、或当 `GOOGLE_APPLICATION_CREDENTIALS` 环境变量指向服务账号 JSON 文件时，ADC 会自动生效。

### rowkey_column [list]

用于接收 Bigtable 行键的字段名列表。未设置时，连接器默认把名为 `rowkey` 的字段当作行键字段。

列出的每个字段会按照其在 `schema.fields` 中声明的类型独立解码：`BYTES` 接收原始行键字节；`STRING` 接收 UTF-8 解码后的视图。因此同一次扫描里的不同行键字段可以使用不同类型（例如一个字段把行键作为原始字节暴露给下游二进制处理，另一个字段同时暴露一个 UTF-8 可读视图）。

### start_rowkey [string]

扫描起始行键，包含该行键。未设置时从表起始位置读取。

连接器会把该值原样以 UTF-8 字符串传给 Bigtable 客户端，只支持字典序比较。对于无法按 UTF-8 编码的二进制行键，请使用 `BYTES` 类型。

### end_rowkey [string]

扫描结束行键，不包含该行键。未设置时读取到表末尾。

### start_timestamp [long]

Cell 时间戳过滤的起始值，包含该时间戳，单位是微秒。与 `end_timestamp`、`max_versions` 配合，可以控制 Bigtable 对每个列限定符返回哪些版本的 Cell。

### end_timestamp [long]

Cell 时间戳过滤的结束值，不包含该时间戳，单位是微秒。

### max_versions [int]

每个列限定符最多返回的 Cell 版本数。默认值 `1` 表示只读取最新版本。更大的值会暴露历史版本，但 Source 仍按 Bigtable 行聚合输出，同一 Cell 的旧版本会被合并到该行返回的最新版本。

### scan_row_limit [int]

每个 split 最多读取的行数。默认值 `-1` 表示不限制。当 Enumerator 切出多个 split 时，作业级上限约为 `scan_row_limit × split 数`，而不是整表一条上限。把 `scan_row_limit` 与 `start_rowkey` / `end_rowkey` 配合，可以在多个作业之间分页扫描整张表。

### common options

Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。

### Schema 映射

字段名须使用 `列族:列限定符` 格式，例如 `cf:name`、`stats:age`。行键字段由 `rowkey_column` 控制；如果未配置，特殊字段名 `rowkey` 会映射到 Bigtable 行键。

| SeaTunnel 字段名 | 映射到 Bigtable |
|-----------------|----------------|
| `rowkey`        | 行键 |
| `cf:name`       | 列族 `cf`，列限定符 `name` |
| `stats:age`     | 列族 `stats`，列限定符 `age` |

:::tip

Source 会读取每个 `列族:列限定符` 字段返回的最新 Cell。可以通过 `start_timestamp`、`end_timestamp` 和 `max_versions` 控制 Bigtable 扫描过滤条件。SeaTunnel 字段类型需要和 Bigtable 中保存的字节格式匹配，例如本连接器写入的数字类型是大端二进制，`STRING`、`DATE`、`TIME`、`TIMESTAMP` 和 `DECIMAL` 是 UTF-8 文本。

:::

## 任务示例

### 使用应用默认凭证读取整张表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleBigtable {
    project_id  = "my-gcp-project"
    instance_id = "my-bigtable-instance"
    table       = "events"
    schema {
      fields {
        rowkey    = BYTES
        "cf:type" = STRING
        "cf:ts"   = BIGINT
      }
    }
  }
}
```

### 使用服务账号扫描行键范围

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleBigtable {
    project_id       = "my-gcp-project"
    instance_id      = "my-bigtable-instance"
    table            = "events"
    credentials_path = "/secrets/sa-key.json"
    start_rowkey     = "2024-01-01#"
    end_rowkey       = "2024-02-01#"
    max_versions     = 1
    schema {
      fields {
        rowkey    = STRING
        "cf:type" = STRING
        "cf:data" = STRING
      }
    }
  }
}
```

### 使用自定义行键字段名

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleBigtable {
    project_id    = "my-gcp-project"
    instance_id   = "my-bigtable-instance"
    table         = "events"
    rowkey_column = ["event_id"]
    schema {
      fields {
        event_id  = STRING
        "cf:type" = STRING
        "cf:data" = STRING
      }
    }
  }
}
```

### 有界流式扫描并按 Cell 版本过滤

在 `STREAMING` 模式下，仍然只做单次有界扫描，但会按 checkpoint 推进。结合 `start_timestamp`、`end_timestamp` 和 `max_versions` 可以限制 Bigtable 返回的 Cell 版本。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  GoogleBigtable {
    project_id      = "my-gcp-project"
    instance_id     = "my-bigtable-instance"
    table           = "events"
    start_timestamp = 1704067200000000
    end_timestamp   = 1735689600000000
    max_versions    = 3
    scan_row_limit  = 500000
    schema {
      fields {
        rowkey    = STRING
        "cf:type" = STRING
        "cf:data" = STRING
        "cf:ts"   = BIGINT
      }
    }
  }
}
```

## Changelog

<ChangeLog />
