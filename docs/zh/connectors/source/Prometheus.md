import ChangeLog from '../changelog/connector-prometheus.md';

# Prometheus

> Prometheus 数据源连接器

## 描述

Prometheus 数据源连接器通过 Prometheus 兼容 HTTP API 读取指标查询结果。它支持即时查询和范围查询，并把每个指标采样点转换为一行 SeaTunnel 数据。

`url` 只需要填写服务基础地址，例如 `http://prometheus:9090` 或 `http://victoria-metrics:8428`。SeaTunnel 会根据
`query_type` 自动使用即时查询接口或范围查询接口。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行处理](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 名称                        | 类型   | 是否必填 | 默认值  | 描述 |
|-----------------------------|--------|----------|---------|------|
| url                         | String | 是       | -       | Prometheus 兼容服务基础地址，例如 `http://prometheus:9090`。 |
| query                       | String | 是       | -       | Prometheus 查询表达式。 |
| query_type                  | String | 否       | Instant | 查询类型，支持 `Instant` 和 `Range`。 |
| start                       | String | 当 `query_type = Range` 时必填 | - | 范围查询的开始时间，支持 RFC3339 时间、Unix 时间戳和 `CURRENT_TIMESTAMP`。 |
| end                         | String | 当 `query_type = Range` 时必填 | - | 范围查询的结束时间，支持 RFC3339 时间、Unix 时间戳和 `CURRENT_TIMESTAMP`。 |
| step                        | String | 当 `query_type = Range` 时必填 | - | 查询步长，例如 `15s`、`1m`，也可以填写秒数。 |
| time                        | Long   | 否       | -       | 即时查询的评估时间，使用 Unix 时间戳。 |
| timeout                     | Long   | 否       | -       | 传给 Prometheus 查询 API 的超时时间。 |
| content_field               | String | 否       | -       | 用于提取指标结果数组的 JSONPath。读取 Prometheus 结果时通常填写 `$.data.result.*`。 |
| format                      | String | 否       | text    | 响应格式。读取 Prometheus 查询结果并配置 schema 时请使用 `json`。 |
| schema                      | Config | 当 `format = json` 时必填 | - | 输出数据结构。 |
| headers                     | Map    | 否       | -       | HTTP 请求头。 |
| params                      | Map    | 否       | -       | 额外的 HTTP 请求参数。SeaTunnel 会自动加入 Prometheus 查询参数。 |
| retry                       | Int    | 否       | -       | HTTP 请求出现 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | Int    | 否       | 100     | 重试退避时间倍数，单位毫秒。 |
| retry_backoff_max_ms        | Int    | 否       | 10000   | 最大重试退避时间，单位毫秒。 |
| poll_interval_millis        | Int    | 否       | -       | 作业以流模式运行时，两次 HTTP 请求之间的间隔，单位毫秒。 |
| common-options              | Config | 否       | -       | 数据源插件通用参数，详情请参考[数据源通用选项](../common-options/source-common-options.md)。 |

## 输出结构

读取 Prometheus 查询结果时，推荐使用下面的固定结构：

```hocon
schema = {
  fields {
    metric = "map<string, string>"
    value = double
    time = long
  }
}
```

连接器会把 Prometheus 采样点转换为这些字段：

| 字段   | 类型                | 描述 |
|--------|---------------------|------|
| metric | map<string, string> | Prometheus 指标标签，包括 `__name__` 等标签。 |
| value  | double              | 指标采样值。 |
| time   | long                | 指标采样时间戳。 |

## 即时查询示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Prometheus {
    plugin_output = "prometheus"
    url = "http://prometheus:9090"
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

## 范围查询示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Prometheus {
    plugin_output = "prometheus"
    url = "http://prometheus:9090"
    query = "up"
    query_type = "Range"
    start = CURRENT_TIMESTAMP
    end = CURRENT_TIMESTAMP
    step = "15s"
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

## Prometheus 兼容接口示例

```hocon
source {
  Prometheus {
    plugin_output = "metrics"
    url = "http://victoria-metrics:8428"
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

## 变更日志

<ChangeLog />
