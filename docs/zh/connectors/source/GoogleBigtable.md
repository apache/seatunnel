import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable Source 连接器

## 描述

使用原生 Bigtable Data v2 Java 客户端从 Google Cloud Bigtable 读取数据。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)

:::tip

该 Source 是有界读取。当前只会为配置的表或行键范围生成一个切分，所以提高作业并行度不会把一次 Bigtable 扫描拆成多个 tablet 范围并发读取。

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

Google Cloud 服务账号 JSON 密钥文件路径。未设置时使用应用默认凭证（ADC）。

### rowkey_column [list]

用于接收 Bigtable 行键的字段名列表。未设置时，连接器默认把名为 `rowkey` 的字段当作行键字段。行键字段可以声明为 `BYTES` 或 `STRING`。

### start_rowkey [string]

扫描起始行键，包含该行键。未设置时从表起始位置读取。

### end_rowkey [string]

扫描结束行键，不包含该行键。未设置时读取到表末尾。

### start_timestamp [long]

Cell 时间戳过滤的起始值，包含该时间戳，单位是微秒。

### end_timestamp [long]

Cell 时间戳过滤的结束值，不包含该时间戳，单位是微秒。

### max_versions [int]

每个列限定符最多返回的 Cell 版本数。默认值 `1` 表示只读取最新版本。

### scan_row_limit [int]

最多读取的行数。默认值 `-1` 表示不限制。

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

## 示例

### 使用应用默认凭证读取整张表

```hocon
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

## Changelog

<ChangeLog />
