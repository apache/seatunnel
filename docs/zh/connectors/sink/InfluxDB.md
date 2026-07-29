import ChangeLog from '../changelog/connector-influxdb.md';

# InfluxDB

> InfluxDB Sink 连接器

## 描述

将 SeaTunnel 行数据写入 InfluxDB 1.x。Sink 会把一行数据转换成一个 InfluxDB point，并通过
`measurement`、`key_time`、`key_tags` 决定写入的 measurement、时间戳、tag 和 field。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

| SeaTunnel 数据类型 | InfluxDB 用法 |
|--------------------|---------------|
| BOOLEAN            | 写为 InfluxDB field。 |
| SMALLINT           | 写为 InfluxDB field。 |
| INT                | 写为 InfluxDB field。 |
| BIGINT             | 写为 InfluxDB field；配置为 `key_time` 时也可以作为时间戳。 |
| FLOAT              | 写为 InfluxDB field。 |
| DOUBLE             | 写为 InfluxDB field。 |
| STRING             | 可写为 InfluxDB field、tag 值，或在配置为 `key_time` 时作为时间戳字符串。 |
| TIMESTAMP          | 可用于 `key_time`，连接器会按 UTC 时区转换成 epoch 毫秒。 |

当前 InfluxDB sink 序列化器不支持其他 SeaTunnel 类型。

## 选项

| 参数名                         | 类型     | 必须 | 默认值          | 描述                                                       |
|-----------------------------|--------|----|--------------|----------------------------------------------------------|
| url                         | string | 是  | -            | InfluxDB 连接 URL，例如 `http://influxdb-host:8086`。          |
| database                    | string | 是  | -            | InfluxDB 数据库名称。                                           |
| measurement                 | string | 否  | 输入表完整名称      | InfluxDB measurement 名称。不配置时使用输入表名。                     |
| username                    | string | 否  | -            | InfluxDB 用户名。必须和 `password` 一起配置。                         |
| password                    | string | 否  | -            | InfluxDB 密码。必须和 `username` 一起配置。                         |
| key_time                    | string | 否  | processing time | 作为 InfluxDB point 时间戳的字段名。不配置时使用处理时间。                 |
| key_tags                    | array  | 否  | -            | 写为 InfluxDB tag 的字段名。其他字段会写为 point field。               |
| batch_size                  | int    | 否  | 1024         | 缓存多少个 point 后写入 InfluxDB。                                |
| max_retries                 | int    | 否  | -            | 写入失败时的最大重试次数。                                            |
| write_timeout               | int    | 否  | 5            | InfluxDB 客户端写入超时时间。                                      |
| retry_backoff_multiplier_ms | int    | 否  | -            | 重试等待时间的倍数，单位毫秒。                                          |
| max_retry_backoff_ms        | int    | 否  | -            | 两次重试之间的最大等待时间，单位毫秒。                                     |
| rp                          | string | 否  | -            | 写入时使用的 retention policy。                                  |
| epoch                       | string | 否  | n            | 客户端使用的时间精度。写入精度识别大写值：`H`、`M`、`S`、`MS`、`U`、`NS`。 |
| connect_timeout_ms          | long   | 否  | 15000        | 连接 InfluxDB 的超时时间，单位毫秒。                                  |
| query_timeout_sec           | int    | 否  | 3            | InfluxDB 客户端读超时时间，单位秒。                                   |
| multi_table_sink_replica    | int    | 否  | -            | 多表写入时的 sink writer 副本数。                                  |
| common-options              | config | 否  | -            | Sink 插件通用参数。                                             |

### url

连接到 influxDB 的 url，例如

```
http://influxdb-host:8086
```

### database [string]

`influxDB` 数据库的名称

### measurement [string]

`influxDB` measurement 的名称。这个配置是可选项；不配置时，sink 会使用输入表完整名称作为
measurement 名称，这在多表写入场景很有用。
多表输入时，请确保生成的表名可以作为合法的 InfluxDB measurement 名称。

### username [string]

`influxDB` 用户名

### password [string]

`influxDB` 用户密码

### key_time [string]

在 SeaTunnelRow 中指定 `influxDB` measurement 时间戳的字段名。如果未指定，则使用处理时间作为时间戳

### key_tags [array]

在 SeaTunnelRow 中指定 `influxDB` measurement 标签的字段名。
如果未指定，则包含所有字段作为 `influxDB` measurement 字段

### batch_size [int]

对于批量写入，当缓冲区数量达到 `batch_size` 数量或时间达到 `checkpoint.interval` 时，数据将被刷新到 influxDB

### max_retries [int]

刷新失败的重试次数

不配置该选项时，sink 只尝试写入一次，失败后直接报错。

### retry_backoff_multiplier_ms [int]

用作生成下一个退避延迟的乘数

### max_retry_backoff_ms [int]

在尝试重新请求 `influxDB` 之前等待的时间量

使用重试配置时，建议同时配置 `retry_backoff_multiplier_ms` 和 `max_retry_backoff_ms`；否则重试等待时间仍为 `0`。

### write_timeout [int]

InfluxDB 客户端写入超时时间。

### rp [string]

写入 point 时使用的 retention policy。

### epoch [string]

InfluxDB 客户端使用的时间精度。用于 sink 写入精度时，当前连接器识别大写值 `H`、`M`、`S`、`MS`、
`U`、`NS`。默认值 `n` 会按纳秒精度处理。

### query_timeout_sec [int]

InfluxDB 客户端读超时时间，单位秒。

### connect_timeout_ms [long]

连接到 InfluxDB 的超时时间，以毫秒为单位

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md) 详见

## 示例

### 写入一个 measurement

```hocon
sink {
    InfluxDB {
        url = "http://influxdb-host:8086"
        database = "test"
        measurement = "sink"
        key_time = "time"
        key_tags = ["label"]
        batch_size = 1
    }
}

```

### 不显式配置 measurement

不配置 `measurement` 时，sink 会使用输入表完整名称作为 measurement 名称。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      table = "influxdb_sink"
      fields {
        label = STRING
        c_string = STRING
        c_double = DOUBLE
        c_bigint = BIGINT
        c_float = FLOAT
        c_int = INT
        c_smallint = SMALLINT
        c_boolean = BOOLEAN
        time = BIGINT
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["label_1", "sink_1", 4.3, 200, 2.5, 2, 5, true, 1627529632356]
      }
    ]
  }
}

sink {
  InfluxDB {
    url = "http://influxdb-host:8086"
    database = "test"
    key_time = "time"
    key_tags = ["label"]
    batch_size = 1
  }
}
```

### 多表写入

不配置 `measurement` 时，每个上游表会写入一个以该表名命名的 measurement，这是多表输入的常用配置方式。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  InfluxDB {
    url = "http://influxdb-host:8086"
    database = "test"
    key_time = "time"
    batch_size = 1
  }
}
```

## 变更日志

<ChangeLog />
