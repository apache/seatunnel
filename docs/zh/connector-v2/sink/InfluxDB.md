# InfluxDB

> InfluxDB数据接收器

## 描述

将数据写入InfluxDB。

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## 选项

|            名称             |  类型  | 是否必传 |        默认值         |
|-----------------------------|--------|----------|------------------------------|
| url                         | string | 是      | -                            |
| database                    | string | 是      |                              |
| measurement                 | string | 是      |                              |
| username                    | string | 否       | -                            |
| password                    | string | 否       | -                            |
| key_time                    | string | 否       | processing time              |
| key_tags                    | array  | 否       | exclude `field` & `key_time` |
| batch_size                  | int    | 否       | 1024                         |
| max_retries                 | int    | 否       | -                            |
| retry_backoff_multiplier_ms | int    | 否       | -                            |
| connect_timeout_ms          | long   | 否       | 15000                        |
| common-options              | config | 否       | -                            |

### url

连接到influexDB的url，例如。

```
http://influxdb-host:8086
```

### database [string]

“influexDB”数据库的名称

### measurement [string]

“influexDB”测量的名称

### username [string]

`influxDB`用户用户名

### password [string]

`influxDB`用户密码

### key_time [string]

在SeaTunnelRow中指定“influexDB”测量时间戳的字段名。如果未指定，则使用处理时间作为时间戳

### key_tags [array]

指定SeaTunnelRow中“influexDB”测量标记的字段名。
如果未指定，请包含所有具有“influexDB”测量字段的字段

### batch_size [int]

对于批写入，当缓冲区的数量达到“batch_size”的数量或时间达到“checkpoint.interval”时，数据将被刷新到influexDB中

### max_retries [int]

刷新的重试次数失败

### retry_backoff_multiplier_ms [int]

用作生成下一个退避延迟的乘数

### max_retry_backoff_ms [int]

在尝试重试对`influexDB的请求之前等待的时间量`

### connect_timeout_ms [long]

连接到InfluxDB的超时时间（毫秒）

### common options

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

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
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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

## 更改日志

### 随后版本

- 添加InfluxDB数据接收器

