import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The GraphQL source connector reads data from a GraphQL endpoint. It supports:

- HTTP batch or polling reads with a GraphQL `query`.
- WebSocket subscription reads with a GraphQL `subscription`.
- JSON response parsing by `content_field` and `schema.fields`.
- Request headers, request parameters, GraphQL variables, and timeout settings.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

In order to use the GraphQL connector, the following dependency is required.
It can be downloaded via install-plugin.sh or from Maven central repository.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| GraphQL    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-graphql) |

## Source Options

| Name                | Type    | Required | Default | Description |
|---------------------|---------|----------|---------|-------------|
| url                 | String  | Yes      | -       | GraphQL endpoint. Use `http://` or `https://` for query mode. Use `ws://` or `wss://` for subscription mode. |
| query               | String  | Yes      | -       | GraphQL statement. Source supports `query`, and supports `subscription` only when `enable_subscription = true`. |
| variables           | Map     | No       | -       | GraphQL variables sent with the request body. |
| enable_subscription | Boolean | No       | false   | Whether to use WebSocket subscription mode. When false, the connector uses HTTP POST. |
| timeout             | Long    | No       | -       | Request timeout value passed to the HTTP client parameter map. |
| headers             | Map     | No       | -       | HTTP request headers, such as authentication headers. |
| params              | Map     | No       | -       | HTTP request parameters. |
| format              | String  | No       | TEXT    | Response format inherited from HTTP source. Use `json` when `schema.fields` is configured. |
| content_field       | String  | No       | -       | JSONPath used to pick the data array or object from the GraphQL response, for example `$.data.source`. |
| schema.fields       | Config  | No       | -       | Output fields and SeaTunnel data types. Configure it when reading structured JSON rows. |
| poll_interval_millis | Int    | No       | -       | Interval between HTTP requests in streaming query mode. |
| max_retries         | Int     | No       | 5       | Maximum reconnect attempts for WebSocket subscription mode. |
| retry_delay_ms      | Int     | No       | 5000    | Delay between WebSocket reconnect attempts, in milliseconds. |
| retry               | Int     | No       | -       | Maximum HTTP retry times when an HTTP request returns an IOException. |
| retry_backoff_multiplier_ms | Int | No   | 100     | HTTP retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms | Int    | No       | 10000   | Maximum HTTP retry backoff in milliseconds. |
| enable_multi_lines  | Boolean | No       | false   | Whether to split the HTTP response by line before parsing. |
| connect_timeout_ms  | Int     | No       | 12000   | HTTP connection timeout in milliseconds. |
| socket_timeout_ms   | Int     | No       | 60000   | HTTP socket timeout in milliseconds. |
| common-options      | Config  | No       | -       | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

## Notes

- In normal query mode, `url` must start with `http://` or `https://`.
- In subscription mode, set `enable_subscription = true` and use a `ws://` or `wss://` URL.
- Source queries cannot be GraphQL `mutation` operations. Use the GraphQL sink for `mutation` requests.
- `content_field` is usually needed because GraphQL responses are commonly wrapped under `data`.
- When `schema.fields` is configured, set `format = "json"` and point `content_field` to the array or object that should become SeaTunnel rows.
- For streaming query mode over HTTP, configure `poll_interval_millis` to control how often SeaTunnel sends the same query again.
- For WebSocket subscription mode, `max_retries` and `retry_delay_ms` only control reconnect behavior after a subscription connection fails.

## Task Example

### Query GraphQL Data

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GraphQL {
    plugin_output = "graphql_source"
    url = "http://graphql:8080/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    query = """
      query MyQuery($limit: Int) {
        source(limit: $limit) {
          id
          val_bool
          val_double
          val_float
        }
      }
    """
    variables = {
      limit = 2
    }
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
        val_double = "double"
        val_float = "float"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_source"
  }
}
```

### Query GraphQL With Authentication Headers

When the GraphQL endpoint requires authentication, pass the bearer token through
`headers`. Any HTTP header supported by the underlying client can be set this way.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GraphQL {
    plugin_output = "graphql_auth"
    url = "https://graphql.example.com/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    headers = {
      Authorization = "Bearer ${secret}"
      X-Tenant = "acme"
    }
    query = """
      query MyQuery {
        source {
          id
          val_bool
        }
      }
    """
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_auth"
  }
}
```

### Streaming Polling Query

For HTTP endpoints that publish new data over time but do not offer a subscription,
run the same query repeatedly in streaming mode. SeaTunnel sends the request every
`poll_interval_millis` and forwards the new rows downstream.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  GraphQL {
    plugin_output = "graphql_streaming"
    url = "http://graphql:8080/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    poll_interval_millis = 10000
    query = """
      query MyQuery {
        source {
          id
          val_bool
          val_double
        }
      }
    """
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
        val_double = "double"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_streaming"
  }
}
```

### Subscribe to GraphQL Data

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  GraphQL {
    plugin_output = "graphql_subscription"
    url = "ws://graphql:8080/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    enable_subscription = true
    max_retries = 5
    retry_delay_ms = 5000
    query = """
      subscription MySubscription {
        source {
          id
          val_bool
          val_double
          val_float
        }
      }
    """
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
        val_double = "double"
        val_float = "float"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_subscription"
  }
}
```

## Changelog

<ChangeLog />
