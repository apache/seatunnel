import ChangeLog from '../changelog/connector-prometheus.md';

# Prometheus

> Prometheus sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Writes metric samples to a Prometheus-compatible remote write endpoint.

The sink converts each SeaTunnel row into one Prometheus sample, serializes the data as a remote write request, compresses it with Snappy, and sends it by HTTP POST. Use a remote write endpoint such as `http://prometheus:9090/api/v1/write` or `http://victoria-metrics:8428/api/v1/write`.

Prometheus may reject samples that are too old for the target server's retention and remote write rules.

## Supported DataSource Info

In order to use the Prometheus connector, the following dependency is required.
It can be downloaded via `install-plugin.sh` or from the Maven central repository.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| Prometheus | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-prometheus) |

## Sink Options

| name                        | type   | required | default value | description |
|-----------------------------|--------|----------|---------------|-------------|
| url                         | String | Yes      | -             | Prometheus-compatible remote write URL. |
| headers                     | Map    | No       | -             | HTTP request headers. |
| retry                       | Int    | No       | -             | Maximum retry times when the HTTP request fails with `IOException`. |
| retry_backoff_multiplier_ms | Int    | No       | 100           | Retry backoff multiplier, in milliseconds. |
| retry_backoff_max_ms        | Int    | No       | 10000         | Maximum retry backoff, in milliseconds. |
| key_label                   | String | Yes      | -             | Field name whose value is used as Prometheus labels. The field value must be a map. |
| key_value                   | String | Yes      | -             | Field name whose value is used as the Prometheus sample value. |
| key_timestamp               | String | No       | -             | Field name whose value is used as the sample timestamp. If omitted, the current system time is used. |
| batch_size                  | Int    | No       | 1024          | Maximum number of samples written in one request. Must be greater than 0. |
| flush_interval              | Long   | No       | 300000        | Scheduled flush interval in milliseconds. |
| common-options              | config | No       | -             | Sink common options. |

### key_label [String]

The named field must be `map<string, string>`. It is converted into Prometheus labels. Include `__name__` in the map to set the metric name.

### key_value [String]

The named field is converted into the Prometheus sample value. A `double` field is recommended.

### key_timestamp [String]

Optional timestamp field.

Supported field types:

- `timestamp`: converted to epoch milliseconds with the local time zone
- `bigint`: treated as epoch milliseconds
- `double`: treated as Unix seconds and converted to milliseconds
- `string`: parsed as epoch milliseconds

When this option is not configured, the sink uses the current system time.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Example

### Write to Prometheus remote write

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
    plugin_output = "fake"
    rows = [
      {
        kind = INSERT
        fields = [{"__name__" : "metric_1"}, 1.23, CURRENT_TIMESTAMP]
      },
      {
        kind = INSERT
        fields = [{"__name__" : "metric_2"}, 1.23, CURRENT_TIMESTAMP]
      }
    ]
  }
}

sink {
  Prometheus {
    plugin_input = "fake"
    url = "http://prometheus:9090/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    batch_size = 1
  }
}
```

### Write to a Prometheus-compatible remote write API

```hocon
sink {
  Prometheus {
    plugin_input = "fake"
    url = "http://victoria-metrics:8428/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    batch_size = 5
  }
}
```

## Changelog

<ChangeLog />
