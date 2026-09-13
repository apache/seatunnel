import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB 写入连接器

## 描述

Amazon DynamoDB 写入连接器用于将 SeaTunnel 数据行写入 DynamoDB 表。

目标表必须提前创建。连接器会把每一行写成一个 DynamoDB item，并使用批量写入请求。它支持单表写入，也支持上游数据行携带表名时的多表写入。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                | 类型   | 必填 | 默认值 | 说明                         |
|---------------------|--------|------|--------|------------------------------|
| url                 | string | 是   | -      | DynamoDB 服务地址。           |
| region              | string | 是   | -      | DynamoDB 所在的 AWS 区域。    |
| access_key_id       | string | 是   | -      | AWS access key ID。           |
| secret_access_key   | string | 是   | -      | AWS secret access key。       |
| table               | string | 是   | -      | 要写入的 DynamoDB 表名。      |
| batch_size          | int    | 否   | 25     | 一次批量写入请求缓存的记录数。 |
| multi_table_sink_replica | int | 否   | -      | 每张表对应的 Sink Writer 副本数。 |
| max_retries         | int    | 否   | 10     | 未处理 item 的最大重试次数，必须大于等于 `0`。 |
| retry_base_delay_ms | long   | 否   | 100    | 初始重试等待时间，单位毫秒。  |
| retry_max_delay_ms  | long   | 否   | 5000   | 最大重试等待时间，单位毫秒。  |
| common-options      | object | 否   | -      | Sink 插件通用参数。           |

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

要写入的 DynamoDB 表名。

普通单表任务中，这里填写目标表名。多表任务中，写入器会优先使用每条数据自带的表名作为目标表；如果数据没有携带表名，则回退使用这里配置的表名。

### batch_size [int]

一次 DynamoDB 批量写入请求缓存的记录数。

DynamoDB batch write 每次最多支持 25 条写请求，所以默认值为 `25`。
不要把该值设置为大于 `25`，否则会超过 DynamoDB batch write API 的限制。

### multi_table_sink_replica [int]

多表写入任务可使用的 Sink 通用选项。更多说明请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

### max_retries [int]

当 DynamoDB 在批量写入结果中返回未处理 item 时，最多重试的次数。
设置为 `0` 表示不重试，该值不能为负数。

### retry_base_delay_ms [long]

重试之间指数退避的基础等待时间，单位为毫秒。

### retry_max_delay_ms [long]

重试之间的最大等待时间，单位为毫秒。

### 通用选项

Sink 连接器通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

## 使用说明

- 启动 SeaTunnel 任务前，需要先创建目标 DynamoDB 表。该写入连接器不会自动创建表或主键结构。
- `access_key_id` 和 `secret_access_key` 是必填项。使用 DynamoDB Local 时，可以填写本地服务接受的占位值。
- DynamoDB 每次 batch write 最多接受 25 条写请求，所以 `batch_size` 不要超过 `25`。
- 写入连接器会对未处理 item 做指数退避重试，但不提供精确一次保证。

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
    max_retries = 10
    retry_base_delay_ms = 100
    retry_max_delay_ms = 5000
  }
}
```

## 变更日志

<ChangeLog />
