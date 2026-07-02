import ChangeLog from '../changelog/connector-prometheus.md';

# Prometheus

> Prometheus 数据源连接器

## 描述

从 Prometheus 兼容的 HTTP API 读取指标样本。

连接器使用 Prometheus 查询 API。`url` 只需要填写基础地址，例如 `http://prometheus:9090` 或 `http://victoria-metrics:8428`。SeaTunnel 会按查询类型自动追加 `/api/v1/query` 或 `/api/v1/query_range`。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 名称                        | 类型      | 是否必填 | 默认值 | 描述 |
|-----------------------------|---------|----------|--------|------|
| url                         | String  | 是       | -      | Prometheus 兼容服务的基础地址。 |
| query                       | String  | 是       | -      | PromQL 查询表达式。 |
| query_type                  | String  | 否       | Instant | 查询类型，可选值为 `Instant` 和 `Range`。 |
| start                       | String  | `query_type = Range` 时必填 | - | 范围查询开始时间。 |
| end                         | String  | `query_type = Range` 时必填 | - | 范围查询结束时间。 |
| step                        | String  | `query_type = Range` 时必填 | - | 范围查询步长，例如 `15s`。 |
| time                        | Long    | 否       | -      | 即时查询的评估时间，使用 Unix 时间戳。 |
| timeout                     | Long    | 否       | -      | 传给 Prometheus 的查询超时时间。 |
| headers                     | Map     | 否       | -      | HTTP 请求头。 |
| params                      | Map     | 否       | -      | 额外的 HTTP 请求参数。SeaTunnel 会自动加入 `query` 和查询时间参数。 |
| content_field               | String  | 否       | -      | 用来提取样本列表的 JSONPath。Prometheus 响应通常填写 `$.data.result.*`。 |
| schema.fields               | Config  | `format = json` 时必填 | - | 输出字段结构。 |
| format                      | String  | 否       | text   | 响应格式。读取 Prometheus 指标样本时请设置为 `json`。 |
| poll_interval_millis        | int     | 否       | -      | 流模式下请求间隔，单位毫秒。 |
| retry                       | int     | 否       | -      | HTTP 请求出现 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | int     | 否       | 100    | 重试退避时间乘数，单位毫秒。 |
| retry_backoff_max_ms        | int     | 否       | 10000  | 最大重试退避时间，单位毫秒。 |
| common-options              | config  | 否       | -      | Source 通用选项。 |

### query_type [String]

`Instant` 表示查询某一个时间点的指标值。`Range` 表示查询一段时间范围内的指标值。

### start / end [String]

仅在 `query_type = Range` 时使用。

支持以下写法：

- `CURRENT_TIMESTAMP`
- ISO-8601 时间，例如 `2025-05-13T02:25:23Z`
- Unix 秒级时间戳，例如 `1747103123.083`

### step [String]

仅在 `query_type = Range` 时使用。它表示 Prometheus 接受的查询步长，例如 `15s`、`1m`，也可以是秒数。

### schema [Config]

Prometheus source 固定输出下面三个字段，顺序也固定：

```hocon
schema = {
  fields {
    metric = "map<string, string>"
    value = double
    time = long
  }
}
```

`metric` 字段保存指标标签，包括 `__name__`。`value` 字段是指标值。`time` 字段是毫秒级样本时间戳。

### common options

源插件通用参数，请参考 [Source Common Options](../common-options/source-common-options.md)。

## 示例

### 即时查询

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Prometheus {
    plugin_output = "prometheus_metrics"
    url = "http://prometheus:9090"
    query = "metric_1"
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

### 范围查询

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Prometheus {
    plugin_output = "prometheus_metrics"
    url = "http://prometheus:9090"
    query = "metric_1"
    query_type = "Range"
    start = "CURRENT_TIMESTAMP"
    end = "CURRENT_TIMESTAMP"
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

### 读取 Prometheus 兼容接口

```hocon
source {
  Prometheus {
    plugin_output = "metrics"
    url = "http://victoria-metrics:8428"
    query = "metric_1"
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
