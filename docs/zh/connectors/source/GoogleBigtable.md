import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable Source 连接器

## 描述

使用原生 Bigtable Data v2 Java 客户端从 Google Cloud Bigtable 读取数据。

## 主要特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

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

### Schema 映射

字段名须使用 `列族:列限定符` 格式，例如 `cf:name`、`stats:age`。特殊字段名 `rowkey` 映射到行键。

## 示例

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

## Changelog

<ChangeLog />
