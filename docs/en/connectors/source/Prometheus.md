import ChangeLog from '../changelog/connector-prometheus.md';

# Prometheus

> Prometheus source connector

## Description

Reads metric samples from Prometheus-compatible HTTP APIs.

The connector uses the Prometheus query API. Configure `url` as the base address, such as `http://prometheus:9090` or `http://victoria-metrics:8428`. SeaTunnel appends `/api/v1/query` for `Instant` queries and `/api/v1/query_range` for `Range` queries.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Options

| name                        | type    | required | default value | description |
|-----------------------------|---------|----------|---------------|-------------|
| url                         | String  | Yes      | -             | Prometheus-compatible server base URL. |
| query                       | String  | Yes      | -             | PromQL expression. |
| query_type                  | String  | No       | Instant       | Query type. Valid values are `Instant` and `Range`. |
| start                       | String  | Required when `query_type = Range` | - | Range query start time. |
| end                         | String  | Required when `query_type = Range` | - | Range query end time. |
| step                        | String  | Required when `query_type = Range` | - | Range query resolution step, for example `15s`. |
| time                        | Long    | No       | -             | Instant query evaluation time, as a Unix timestamp. |
| timeout                     | Long    | No       | -             | Query timeout passed to Prometheus. |
| headers                     | Map     | No       | -             | HTTP request headers. |
| params                      | Map     | No       | -             | Extra HTTP request parameters. SeaTunnel adds `query` and query time parameters automatically. |
| content_field               | String  | No       | -             | JSONPath used to extract the sample list. For Prometheus responses, use `$.data.result.*`. |
| schema.fields               | Config  | Required when `format = json` | - | Output schema. |
| format                      | String  | No       | text          | Response format. Use `json` for Prometheus metric samples. |
| poll_interval_millis        | int     | No       | -             | Request interval in stream mode, in milliseconds. |
| retry                       | int     | No       | -             | Maximum retry times when the HTTP request fails with `IOException`. |
| retry_backoff_multiplier_ms | int     | No       | 100           | Retry backoff multiplier, in milliseconds. |
| retry_backoff_max_ms        | int     | No       | 10000         | Maximum retry backoff, in milliseconds. |
| common-options              | config  | No       | -             | Source common options. |

### query_type [String]

`Instant` evaluates the query at a single time. `Range` evaluates the query over a time range.

### start / end [String]

Used only when `query_type = Range`.

Supported values:

- `CURRENT_TIMESTAMP`
- ISO-8601 timestamp, for example `2025-05-13T02:25:23Z`
- Unix timestamp in seconds, for example `1747103123.083`

### step [String]

Used only when `query_type = Range`. It is the query resolution step accepted by Prometheus, such as `15s`, `1m`, or a number of seconds.

### schema [Config]

Prometheus source returns three fields in this order:

```hocon
schema = {
  fields {
    metric = "map<string, string>"
    value = double
    time = long
  }
}
```

The `metric` field contains labels, including `__name__`. The `value` field is the metric value. The `time` field is the sample timestamp in milliseconds.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

### Instant query

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

### Range query

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

### Read from a Prometheus-compatible API

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

## Changelog

<ChangeLog />
