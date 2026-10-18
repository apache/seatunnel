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
| retry                       | Int    | No       | 3       | Maximum retry attempts for a failed remote-write request. Retries transport `IOException`s and retryable HTTP statuses (`5xx` and `429`); other `4xx` responses fail fast. Set to `0` to disable retries. |
| retry_backoff_multiplier_ms | Int    | No       | 100     | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | Int    | No       | 10000   | Maximum retry backoff in milliseconds. |
| batch_size                  | Int    | No       | 1024    | Positive number of rows buffered before writing to Prometheus. |
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

### Timer Flush

The sink can flush its buffer on a timer so that buffered samples are sent even when the upstream
flow is idle and fewer than `batch_size` rows have been buffered. This timer is driven by the
engine, not by the connector, and is currently supported only by **SeaTunnel Zeta**.

Enable it by setting `sink.flush.interval` (milliseconds) in the job `env` block:

```hocon
env {
  sink.flush.interval = 10000
}
```

The engine then triggers the flush on the normal sink input-processing path, so there is no
connector-owned background thread and no concurrency between the timer flush and the write,
checkpoint, or close paths. A flush that fails is propagated to the engine instead of being silently
dropped.

> On Spark and Flink there is no sub-checkpoint timer flush: `sink.flush.interval` is a Zeta engine
> primitive, and the Spark/Flink sink writer context does not implement it. On those engines the
> buffer is flushed when it reaches `batch_size`, on checkpoint (`PrometheusWriter` flushes in
> `prepareCommit()`), and when the writer is closed. Buffered samples are therefore bounded by the
> checkpoint interval rather than held until `batch_size` or close. For lower latency between
> checkpoints on Spark or Flink, tune `batch_size` accordingly.

The checkpoint flush runs on all engines, including Zeta. So on Zeta the buffer is flushed by both
`sink.flush.interval` and each checkpoint: if the checkpoint interval is shorter than
`sink.flush.interval`, flushes happen more often (in smaller batches) than the timer alone. This is
expected; tune `sink.flush.interval` and the checkpoint interval together if request cadence matters.

### Checkpoint Flush and Failure Handling

The checkpoint flush is a single remote-write request, and a failed flush fails the checkpoint rather
than dropping the batch. The sink retries transient failures before giving up, and tolerates the
replay case:

- **Transient failures are retried.** A transport error (connection refused, reset, timeout) or a
  retryable HTTP status (`5xx` or `429`) is retried up to `retry` times with exponential backoff
  (`retry_backoff_multiplier_ms`, capped at `retry_backoff_max_ms`); only once the retries are
  exhausted does the flush fail the checkpoint. Other `4xx` responses are not retryable and fail fast.
  On Flink the default `tolerableCheckpointFailureNumber` is `0`, so an exhausted-retry failure
  restarts the job; for a low-throughput job on Spark or Flink you may also want to raise that engine
  setting.
- **Replay after a failed checkpoint is tolerated.** After a failed checkpoint the job restarts and
  the source replays from the last successful checkpoint, so the buffered samples are re-sent. If the
  remote-write receiver rejects a re-sent sample as a duplicate (same labels and timestamp) or as
  out-of-order (Prometheus TSDB, and receivers such as Cortex, Mimir, and Thanos, return `400` for
  these), the sink treats that `400` as delivered rather than failing, so a replay does not loop the
  job. The delivery guarantee remains at-least-once. This is a best-effort match on receiver-specific
  error wording; a receiver that returns `400` with different wording is not recognized and the flush
  fails as with any other `4xx`, and each tolerated rejection is logged at `WARN`.

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

## Streaming Remote Write With Batched Flush

This example reads from Kafka in streaming mode and writes to a Prometheus remote
write endpoint. The sink buffers up to `batch_size` rows before issuing the HTTP
write, and the engine-level `sink.flush.interval` (Zeta only) flushes the buffer
every 10 seconds so that samples are still sent when the upstream flow is idle.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
  sink.flush.interval = 10000
}

source {
  Kafka {
    plugin_output = "metrics_topic"
    bootstrap.servers = "kafka:9092"
    topic = "metrics"
    format = "json"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_double = double
        c_timestamp = bigint
      }
    }
  }
}

sink {
  Prometheus {
    plugin_input = "metrics_topic"
    url = "http://prometheus:9090/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    batch_size = 2048
    retry = 5
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 10000
  }
}
```

## Multi-Table Remote Write

When a single job reads from multiple upstream tables and writes to a single
Prometheus remote write endpoint, set `multi_table_sink_replica` to control how
many writer tasks each table gets. The default of `1` is fine when tables are
small; raise it only when one table needs more parallelism than the others.

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake_app_a"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_double = double
        c_timestamp = timestamp
      }
    }
    rows = [
      { kind = INSERT, fields = [{"__name__" : "app_a_metric"}, 1.0, CURRENT_TIMESTAMP] }
    ]
  }
  FakeSource {
    plugin_output = "fake_app_b"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_double = double
        c_timestamp = timestamp
      }
    }
    rows = [
      { kind = INSERT, fields = [{"__name__" : "app_b_metric"}, 2.0, CURRENT_TIMESTAMP] }
    ]
  }
}

sink {
  Prometheus {
    plugin_input = ["fake_app_a", "fake_app_b"]
    url = "http://prometheus:9090/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    multi_table_sink_replica = 2
  }
}
```

## Changelog

<ChangeLog />
