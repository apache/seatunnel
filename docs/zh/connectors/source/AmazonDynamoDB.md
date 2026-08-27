import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB 源连接器

## 描述

Amazon DynamoDB 源连接器通过 DynamoDB scan 请求读取已有表中的数据。

该连接器是批处理源。DynamoDB 不像关系型数据库那样提供完整字段类型信息，所以必须在 SeaTunnel 中显式配置 schema。

该源连接器使用 scan 请求读取表中当前已有的数据，不读取 DynamoDB Streams 或 CDC 变更事件。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                  | 类型   | 必填 | 默认值 | 说明                       |
|-----------------------|--------|------|--------|----------------------------|
| url                   | string | 是   | -      | DynamoDB 服务地址。         |
| region                | string | 是   | -      | DynamoDB 所在的 AWS 区域。  |
| access_key_id         | string | 是   | -      | AWS access key ID。         |
| secret_access_key     | string | 是   | -      | AWS secret access key。     |
| table                 | string | 是   | -      | 要扫描的 DynamoDB 表名。    |
| schema                | config | 是   | -      | 要读取的 SeaTunnel 字段。   |
| scan_item_limit       | int    | 否   | 1      | 每次 scan 请求返回的最大 item 数。 |
| parallel_scan_threads | int    | 否   | 2      | parallel scan 的逻辑分片数。 |
| common-options        | object | 否   | -      | 源插件通用参数。       |

### url [string]

DynamoDB 服务地址，例如 `https://dynamodb.us-east-1.amazonaws.com`。

如果使用 DynamoDB Local 测试，可以填写本地地址，例如 `http://127.0.0.1:8000`。

### region [string]

DynamoDB 所在的 AWS 区域，例如 `us-east-1`。

### access_key_id [string]

连接 DynamoDB 使用的 AWS access key ID。

### secret_access_key [string]

连接 DynamoDB 使用的 AWS secret access key。

### table [string]

要扫描的 DynamoDB 表名。

### schema [config]

定义需要从 DynamoDB item 中读取的 SeaTunnel 字段。

DynamoDB 是键值和文档数据库，源连接器无法从 DynamoDB 自动推断完整的 SeaTunnel schema，所以需要在这里列出所有要读取的字段。

```hocon
schema = {
  fields {
    id = string
    c_map = "map<string, smallint>"
    c_array = "array<tinyint>"
    c_string = string
    c_boolean = boolean
    c_int = int
    c_bigint = bigint
    c_float = float
    c_double = double
    c_decimal = "decimal(2, 1)"
    c_bytes = bytes
    c_date = date
    c_timestamp = timestamp
  }
}
```

更多 schema 写法请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### scan_item_limit [int]

每次 DynamoDB scan 请求最多返回的 item 数量。

较大的值可以减少请求次数，但也会增加每个读取批次占用的内存。

### parallel_scan_threads [int]

DynamoDB parallel scan 使用的逻辑分片数量。

这个值会影响源连接器如何拆分表扫描任务，通常需要结合任务并行度和表数据量设置。

小表通常保留默认值即可。大表可以结合 `env.parallelism` 和 Source 的 `parallelism` 一起调大，让多个 reader 扫描不同分片。

### 通用选项

源连接器通用参数，请参考[源通用选项](../common-options/source-common-options.md)。

## 使用说明

- 该源连接器使用 DynamoDB scan 请求，所以读取的是当前表快照，不是变更事件。
- `access_key_id` 和 `secret_access_key` 是必填项。使用 DynamoDB Local 时，可以填写本地服务接受的占位值。
- `parallel_scan_threads` 控制 DynamoDB scan 的逻辑分片数。大表可以结合任务并行度一起调大。
- `scan_item_limit` 是每次 scan 请求的分页限制，不是整个任务最多读取的总行数。

## 数据类型映射

| SeaTunnel 数据类型 | DynamoDB 属性类型 |
|--------------------|-------------------|
| BOOLEAN            | BOOL              |
| TINYINT            | N                 |
| SMALLINT           | N                 |
| INT                | N                 |
| BIGINT             | N                 |
| FLOAT              | N                 |
| DOUBLE             | N                 |
| DECIMAL            | N                 |
| STRING             | S                 |
| TIME               | S                 |
| DATE               | S                 |
| TIMESTAMP          | S                 |
| BYTES              | B                 |
| MAP                | M                 |
| ARRAY              | L                 |
| NULL               | NULL              |

## 任务示例

下面的示例从 `source_table` 读取数据，并写入 `sink_table`。

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "source_table"
    parallelism = 2
    scan_item_limit = 2
    parallel_scan_threads = 4
    schema = {
      fields {
        id = string
        c_map = "map<string, smallint>"
        c_array = "array<tinyint>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(2, 1)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "sink_table"
    batch_size = 25
  }
}
```

## 变更日志

<ChangeLog />
