# Prometheus

> Prometheus 数据接收器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## 描述

接收Source端传入的数据，利用数据触发 web hooks。

> 例如，来自上游的数据为 [`label: {"__name__": "test1"}, value: 1.2.3,time:2024-08-15T17:00:00`], 则body内容如下: `{"label":{"__name__": "test1"}, "value":"1.23","time":"2024-08-15T17:00:00"}`

**Tips: Prometheus 数据接收器 仅支持 `post json` 类型的 web hook，source 数据将被视为 webhook 中的 body 内容。并且不支持传递过去太久的数据**

## 支持的数据源信息

想使用 Prometheus 连接器，需要安装以下必要的依赖。可以通过运行 install-plugin.sh 脚本或者从 Maven 中央仓库下载这些依赖

| 数据源  |   支持版本    |                                                        依赖                                                        |
|------|-----------|------------------------------------------------------------------------------------------------------------------|
| Http | universal | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-prometheus) |

## 接收器选项

| Name                        | Type   | Required | Default | Description                                                       |
|-----------------------------|--------|----------|---------|-------------------------------------------------------------------|
| url                         | String | Yes      | -       | Http 请求链接                                                         |
| headers                     | Map    | No       | -       | Http 标头                                                           |
| retry                       | Int    | No       | -       | 如果请求http返回`IOException`的最大重试次数                                    |
| retry_backoff_multiplier_ms | Int    | No       | 100     | http请求失败，重试回退次数（毫秒）乘数                                             |
| retry_backoff_max_ms        | Int    | No       | 10000   | http请求失败，最大重试回退时间(毫秒)                                             |
| connect_timeout_ms          | Int    | No       | 12000   | 连接超时设置，默认12s                                                      |
| socket_timeout_ms           | Int    | No       | 60000   | 套接字超时设置，默认为60s                                                    |
| key_timestamp               | Int    | NO       | -       | prometheus时间戳的key.                                                |
| key_label                   | String | yes      | -       | prometheus标签的key                                                  |
| key_value                   | Double | yes      | -       | prometheus值的key                                                   |
| batch_size                  | Int    | false    | 1024       | prometheus批量写入大小                                                  |
| flush_interval              | Long   | false      | 300000L  | prometheus定时写入  |
| common-options              |        | No       | -       | Sink插件常用参数，请参考 [Sink常用选项 ](../sink-common-options.md) 了解详情        |

## 示例

简单示例:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_double = double
        c_timestamp = timestamp
      }
    }
    result_table_name = "fake"
    rows = [
       {
         kind = INSERT
         fields = [{"__name__": "test1"},  1.23, "2024-08-15T17:00:00"]
       },
       {
         kind = INSERT
         fields = [{"__name__": "test2"},  1.23, "2024-08-15T17:00:00"]
       }
    ]
  }
}


sink {
  Prometheus {
    url = "http://prometheus:9090/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    batch_size = 1
  }
}
```

## Changelog

### 2.3.8-beta 2024-08-22

- 添加prometheus接收连接器

