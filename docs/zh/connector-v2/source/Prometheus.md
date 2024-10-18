# Prometheus

> Prometheus 数据源连接器

## 描述

用于读取prometheus数据。

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [并行](../../concept/connector-v2-features.md)

## 源选项

| 名称                          | 类型      | 是否必填 | 默认值             |
|-----------------------------|---------|------|-----------------|
| url                         | String  | Yes  | -               |
| query                       | String  | Yes  | -               |
| query_type                  | String  | Yes  | Instant         |
| content_field               | String  | Yes  | $.data.result.* |
| schema.fields               | Config  | Yes  | -               |
| format                      | String  | No   | json            |
| params                      | Map     | Yes  | -               |
| poll_interval_millis        | int     | No   | -               |
| retry                       | int     | No   | -               |
| retry_backoff_multiplier_ms | int     | No   | 100             |
| retry_backoff_max_ms        | int     | No   | 10000           |
| enable_multi_lines          | boolean | No   | false           |
| common-options              | config  | No   |                 |

### url [String]

http 请求路径。

### query [String]

Prometheus 表达式查询字符串

### query_type [String]

Instant/Range

1. Instant : 简单指标的即时查询。
2. Range : 一段时间内指标数据。

https://prometheus.io/docs/prometheus/latest/querying/api/

### params [Map]

http 请求参数

### poll_interval_millis [int]

流模式下请求HTTP API间隔(毫秒)

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

请求http返回到' IOException '的最大重试次数

### retry_backoff_max_ms [int]

http请求失败，最大重试回退时间(毫秒)

### format [String]

上游数据的格式，默认为json。

### schema [Config]

按照如下填写一个固定值

```hocon
    schema = {
        fields {
            metric = "map<string, string>"
            value = double
            time = long
            }
        }

```

#### fields [Config]

上游数据的模式字段

### common options

源插件常用参数，请参考[Source Common Options](../source-common-options.md) 了解详细信息

## 示例

### Instant:

```hocon
source {
  Prometheus {
    result_table_name = "http"
    url = "http://mockserver:1080"
    query = "up"
    query_type = "Instant"
    content_field = "$.data.result.*"
    format = "json"
    schema = {
        fields {
            metric = "map<string, string>"
            value = double
            time = long
            }
        }
    }
}
```

### Range

```hocon
source {
  Prometheus {
    result_table_name = "http"
    url = "http://mockserver:1080"
    query = "up"
    query_type = "Range"
    content_field = "$.data.result.*"
    format = "json"
    start = "2024-07-22T20:10:30.781Z"
    end = "2024-07-22T20:11:00.781Z"
    step = "15s"
    schema = {
        fields {
            metric = "map<string, string>"
            value = double
            time = long
            }
        }
    }
  }
```

## Changelog

### next version

- 添加Prometheus源连接器
- 减少配置项

