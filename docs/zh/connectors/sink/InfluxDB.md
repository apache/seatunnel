import ChangeLog from '../changelog/connector-influxdb.md';

# InfluxDB

> InfluxDB Sink 连接器

## 描述

将数据写入 InfluxDB。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 选项

|            参数名             |  类型  | 必须 |        默认值         |
|-----------------------------|--------|------|------------------------------|
| url                         | string | 是   | -                            |
| database                    | string | 是   |                              |
| measurement                 | string | 是   |                              |
| username                    | string | 否   | -                            |
| password                    | string | 否   | -                            |
| key_time                    | string | 否   | processing time              |
| key_tags                    | array  | 否   | exclude `field` & `key_time` |
| batch_size                  | int    | 否   | 1024                         |
| max_retries                 | int    | 否   | -                            |
| retry_backoff_multiplier_ms | int    | 否   | -                            |
| connect_timeout_ms          | long   | 否   | 15000                        |
| common-options              | config | 否   | -                            |

### url

连接到 influxDB 的 url，例如

```
http://influxdb-host:8086
```

### database [string]

`influxDB` 数据库的名称

### measurement [string]

`influxDB` measurement 的名称

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

### retry_backoff_multiplier_ms [int]

用作生成下一个退避延迟的乘数

### max_retry_backoff_ms [int]

在尝试重新请求 `influxDB` 之前等待的时间量

### connect_timeout_ms [long]

连接到 InfluxDB 的超时时间，以毫秒为单位

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md) 详见

## 示例

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

### 多表

#### 示例1

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
    measurement = "${table_name}_test"
  }
}
```

## 变更日志

<ChangeLog />


