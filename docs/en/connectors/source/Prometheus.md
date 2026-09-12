import ChangeLog from '../changelog/connector-prometheus.md';

# Prometheus

> Prometheus source connector

## Description

The Prometheus source connector reads metric query results from Prometheus-compatible HTTP APIs. It supports instant
queries and range queries, and returns each metric sample as a SeaTunnel row.

Configure `url` as the server base address, such as `http://prometheus:9090` or `http://victoria-metrics:8428`.
SeaTunnel automatically uses the Prometheus query endpoint for `Instant` queries and the query range endpoint for
`Range` queries.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

To use the Prometheus connector, the following dependency is required. It can be installed by `install-plugin.sh` or
downloaded from Maven Central.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| Prometheus | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-prometheus) |

## Source Options

| Name                        | Type   | Required | Default | Description |
|-----------------------------|--------|----------|---------|-------------|
| url                         | String | Yes      | -       | Prometheus-compatible server base URL, for example `http://prometheus:9090`. |
| query                       | String | Yes      | -       | Prometheus expression query. |
| query_type                  | String | No       | Instant | Query type. Supported values are `Instant` and `Range`. |
| start                       | String | Required when `query_type = Range` | - | Start timestamp for a range query. Supports RFC3339 timestamps, Unix timestamps, and `CURRENT_TIMESTAMP`. |
| end                         | String | Required when `query_type = Range` | - | End timestamp for a range query. Supports RFC3339 timestamps, Unix timestamps, and `CURRENT_TIMESTAMP`. |
| step                        | String | Required when `query_type = Range` | - | Query resolution step width, such as `15s`, `1m`, or a float number of seconds. |
| time                        | Long   | No       | -       | Evaluation timestamp for an instant query, as a Unix timestamp. |
| timeout                     | Long   | No       | -       | Evaluation timeout passed to the Prometheus query API. |
| content_field               | String | No       | -       | JSONPath used to extract the array of metric results. For Prometheus responses, usually `$.data.result.*`. |
| format                      | String | No       | text    | Response format. Use `json` when reading Prometheus query results with a schema. |
| schema                      | Config | Required when `format = json` | - | Output schema. |
| headers                     | Map    | No       | -       | HTTP request headers. |
| params                      | Map    | No       | -       | Extra HTTP request parameters. SeaTunnel adds the Prometheus query parameters automatically. |
| retry                       | Int    | No       | -       | Maximum retry times when the HTTP request throws an `IOException`. |
| retry_backoff_multiplier_ms | Int    | No       | 100     | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | Int    | No       | 10000   | Maximum retry backoff in milliseconds. |
| poll_interval_millis        | Int    | No       | -       | HTTP request interval in milliseconds when the job runs in stream mode. |
| common-options              | Config | No       | -       | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

## Output Schema

For Prometheus query results, use the following schema:

```hocon
schema = {
  fields {
    metric = "map<string, string>"
    value = double
    time = long
  }
}
```

The connector converts a Prometheus sample into these fields:

| Field  | Type                | Description |
|--------|---------------------|-------------|
| metric | map<string, string> | Prometheus metric labels, including labels such as `__name__`. |
| value  | double              | Sample value. |
| time   | long                | Sample timestamp. |

## Instant Query Example

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

## Range Query Example

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

## Prometheus-Compatible API Example

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

## Streaming Range Query Example

Run a range query continuously against a Prometheus or VictoriaMetrics server. The
source re-runs the same fixed time-window query every `poll_interval_millis`.
`start` and `end` are resolved when the job starts, so the time window does not
advance between polls.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Prometheus {
    plugin_output = "live_metrics"
    url = "http://prometheus:9090"
    query = "rate(node_cpu_seconds_total{mode!=\"idle\"}[1m])"
    query_type = "Range"
    start = "2026-08-10T00:00:00Z"
    end = CURRENT_TIMESTAMP
    step = "30s"
    content_field = "$.data.result.*"
    format = "json"
    poll_interval_millis = 15000
    retry = 3
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 5000
    schema = {
      fields {
        metric = "map<string, string>"
        value = double
        time = long
      }
    }
  }
}

sink {
  Console {}
}
```

## Range Query With Explicit Unix Timestamps

Both `start` and `end` accept Unix timestamps in seconds. This is useful when you
want to backfill a fixed window from a known point in time.

```hocon
source {
  Prometheus {
    plugin_output = "backfill"
    url = "http://prometheus:9090"
    query = "sum(up) by (job)"
    query_type = "Range"
    start = "1754851200"
    end = "1754937600"
    step = "60s"
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
