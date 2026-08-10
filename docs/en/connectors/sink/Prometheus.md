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

The Prometheus sink connector writes rows to the Prometheus remote write API. It builds a remote write sample from
three upstream fields:

- `key_label`: the field that contains Prometheus labels, usually a `map<string, string>`.
- `key_value`: the numeric sample value field.
- `key_timestamp`: the optional timestamp field.

The sink serializes rows as Prometheus remote write samples, compresses the request with Snappy, and sends data by
HTTP `POST` to a Prometheus-compatible remote write endpoint such as `http://prometheus:9090/api/v1/write` or
`http://victoria-metrics:8428/api/v1/write`.

Prometheus-compatible servers may reject samples that are too old for their retention or remote write rules.

## Supported DataSource Info

To use the Prometheus connector, the following dependency is required. It can be installed by `install-plugin.sh` or
downloaded from Maven Central.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| Prometheus | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-prometheus) |

## Sink Options

| Name                        | Type   | Required | Default | Description |
|-----------------------------|--------|----------|---------|-------------|
| url                         | String | Yes      | -       | Prometheus-compatible remote write API URL, for example `http://prometheus:9090/api/v1/write`. |
| key_label                   | String | Yes      | -       | Name of the upstream field that contains Prometheus labels. The field value should be a map. |
| key_value                   | String | Yes      | -       | Name of the upstream field that contains the Prometheus sample value. A `double` field is recommended. |
| key_timestamp               | String | No       | -       | Name of the upstream field that contains the Prometheus sample timestamp. If omitted, the sink uses the current system time. |
| headers                     | Map    | No       | -       | HTTP request headers. |
| retry                       | Int    | No       | -       | Maximum retry times when the HTTP request throws an `IOException`. |
| retry_backoff_multiplier_ms | Int    | No       | 100     | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | Int    | No       | 10000   | Maximum retry backoff in milliseconds. |
| batch_size                  | Int    | No       | 1024    | Positive number of rows buffered before writing to Prometheus. |
| flush_interval              | Long   | No       | 300000  | Maximum flush interval in milliseconds. |
| multi_table_sink_replica    | Int    | No       | 1       | Writer replica count for each table in a multi-table sink job. |
| common-options              | Config | No       | -       | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md). |

### key_label

The named field should be `map<string, string>`. It is converted to Prometheus labels. Include `__name__` in the map
to set the metric name.

The sink adds the required remote write headers automatically: `Content-type`,
`Content-Encoding`, and `X-Prometheus-Remote-Write-Version`.

### key_timestamp

Supported timestamp field types:

- `timestamp`: converted to epoch milliseconds with the local time zone
- `bigint`: treated as epoch milliseconds
- `double`: treated as Unix seconds and converted to milliseconds
- `string`: parsed as epoch milliseconds

### multi_table_sink_replica

Replica count for multi-table sink writers. It applies to each table in a multi-table job. Keep the
default value `1` unless one table needs more writer parallelism.

## Example

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

## Prometheus-Compatible Remote Write Example

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
