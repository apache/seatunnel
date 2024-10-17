# Prometheus

> Prometheus source connector

## Description

Used to read data from Prometheus.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)

## Options

|            name             |  type   | required |  default value  |
|-----------------------------|---------|----------|-----------------|
| url                         | String  | Yes      | -               |
| query                       | String  | Yes      | -               |
| query_type                  | String  | Yes      | Instant         |
| content_field               | String  | Yes      | $.data.result.* |
| schema.fields               | Config  | Yes      | -               |
| format                      | String  | No       | json            |
| params                      | Map     | Yes      | -               |
| poll_interval_millis        | int     | No       | -               |
| retry                       | int     | No       | -               |
| retry_backoff_multiplier_ms | int     | No       | 100             |
| retry_backoff_max_ms        | int     | No       | 10000           |
| enable_multi_lines          | boolean | No       | false           |
| common-options              | config  | No       | -               |

### url [String]

http request url

### query [String]

Prometheus expression query string

### query_type [String]

Instant/Range

1. Instant : The following endpoint evaluates an instant query at a single point in time
2. Range : The following endpoint evaluates an expression query over a range of time

https://prometheus.io/docs/prometheus/latest/querying/api/

### params [Map]

http request params

### poll_interval_millis [int]

request http api interval(millis) in stream mode

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

The retry-backoff times(millis) multiplier if request http failed

### retry_backoff_max_ms [int]

The maximum retry-backoff times(millis) if request http failed

### format [String]

the format of upstream data, default `json`.

### schema [Config]

Fill in a fixed value

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

the schema fields of upstream data

### common options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details

## Example

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

- Add Prometheus Source Connector
- Reduce configuration items

