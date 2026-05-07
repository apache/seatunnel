import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable Sink 连接器

## 描述

使用原生 Bigtable Data v2 Java 客户端将数据写入 Google Cloud Bigtable。

## 主要特性

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)

## 参数

| 参数名               | 类型    | 是否必填 | 默认值 |
|--------------------|---------|--------|------|
| project_id         | string  | 是     | -    |
| instance_id        | string  | 是     | -    |
| table              | string  | 是     | -    |
| rowkey_column      | list    | 是     | -    |
| column_family      | config  | 是     | -    |
| credentials_path   | string  | 否     | -    |
| rowkey_delimiter   | string  | 否     | ""   |
| version_column     | string  | 否     | -    |
| null_mode          | string  | 否     | skip |
| batch_mutation_size| int     | 否     | 100  |
| common-options     |         | 否     | -    |

### project_id [string]

Google Cloud 项目 ID，例如 `"my-gcp-project"`。

### instance_id [string]

Bigtable 实例 ID，例如 `"my-bigtable-instance"`。

### table [string]

写入的 Bigtable 表名，例如 `"my-table"`。

### rowkey_column [list]

用于构造行键的列名列表，例如 `["id"]` 或 `["tenant_id", "event_id"]`。多列时用 `rowkey_delimiter` 拼接。

### column_family [config]

列名到列族的映射配置。可使用 `all_columns` 作为默认列族：

```hocon
column_family {
  all_columns = "cf"
}
```

也可以为不同列指定不同列族：

```hocon
column_family {
  name = "info"
  age  = "stats"
}
```

### credentials_path [string]

Google Cloud 服务账号 JSON 密钥文件路径。未设置时使用应用默认凭证（ADC）。

### rowkey_delimiter [string]

多列行键的拼接分隔符，默认为空字符串 `""`。

### version_column [string]

用作 Bigtable Cell 时间戳（微秒）的 BIGINT 列名。未设置时使用当前系统时间。

### null_mode [string]

空值写入策略：`skip`（默认，跳过该 Cell）或 `empty`（写入空字节数组）。

### batch_mutation_size [int]

每次批量提交的行数，默认 `100`。

## 示例

```hocon
sink {
  GoogleBigtable {
    project_id  = "my-gcp-project"
    instance_id = "my-bigtable-instance"
    table       = "events"
    rowkey_column = ["event_id"]
    column_family {
      all_columns = "cf"
    }
  }
}
```

## Changelog

<ChangeLog />
