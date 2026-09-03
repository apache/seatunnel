import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The GraphQL sink connector writes SeaTunnel rows to a GraphQL service by sending a GraphQL
`mutation` over HTTP POST. For each input row, the sink adds row fields to GraphQL `variables`
with the same field names, then sends the configured mutation.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

In order to use the GraphQL connector, the following dependency is required.
It can be downloaded via install-plugin.sh or from Maven central repository.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| GraphQL    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-graphql) |

## Sink Options

| Name          | Type    | Required | Default | Description |
|---------------|---------|----------|---------|-------------|
| url           | String  | Yes      | -       | GraphQL HTTP endpoint. Sink mode requires `http://` or `https://`. |
| query         | String  | Yes      | -       | GraphQL mutation statement. Sink only supports `mutation`. |
| variables     | Map     | No       | -       | Initial GraphQL variables. Input row fields are added to this map before each request. |
| valueCover    | Boolean | No       | false   | When false, row fields overwrite variables with the same name. When true, existing variables keep their configured values. |
| timeout       | Long    | No       | -       | Request timeout value passed to the HTTP client parameter map. |
| headers       | Map     | No       | -       | HTTP request headers, such as authentication headers. |
| params        | Map     | No       | -       | HTTP request parameters. |
| retry         | Int     | No       | -       | Maximum retry times when an HTTP request returns an IOException. |
| retry_backoff_multiplier_ms | Int | No | 100 | HTTP retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms | Int | No | 10000 | Maximum HTTP retry backoff in milliseconds. |
| connect_timeout_ms | Int | No | 12000 | HTTP connection timeout in milliseconds. |
| socket_timeout_ms | Int | No | 60000 | HTTP socket timeout in milliseconds. |
| multi_table_sink_replica | Int | No | - | Sink common option. It controls sink replica count in multi-table runtime. The same GraphQL mutation is still used for all rows routed to this sink. |
| common-options | Config | No | - | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md). |

## Notes

- The sink validates that `query` is a GraphQL `mutation`.
- Field names in the upstream row should match variable names used by the mutation.
- If `variables` already contains a key and `valueCover = true`, the configured variable value is kept.
- If `valueCover = false`, row field values replace variables with the same name.
- The sink sends one mutation request for each input row. If the upstream source contains multiple tables, make sure the same mutation and variable names can handle every table routed to this sink.
- `multi_table_sink_replica` only affects the multi-table sink runtime replica count. It does not choose a different GraphQL mutation per table.

### Authentication

Most GraphQL services require an authentication header. Pass it through `headers`:

```hocon
sink {
  GraphQL {
    plugin_input = "fake"
    url = "https://graphql.example.com/v1/graphql"
    headers = {
      Authorization = "Bearer ${secret}"
    }
    query = """
      mutation MyMutation($id: Int!, $val_string: String!) {
        insert_event(objects: {id: $id, val_string: $val_string}) {
          affected_rows
        }
      }
    """
  }
}
```

`password`, `token`, and similar sensitive values should be supplied via the
job substitution mechanism (for example `${secret}` resolved by the runtime),
not inlined into the configuration file.

## Task Example

### Write Rows with a GraphQL Mutation

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        id = int
        val_bool = boolean
        val_int8 = tinyint
        val_int16 = smallint
        val_int32 = int
        val_int64 = bigint
        val_float = float
        val_double = double
        val_decimal = "decimal(16, 1)"
        val_string = string
        val_unixtime_micros = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, true, 1, 2, 3, 4, 4.3, 5.3, 6.3, "NEW", "2020-02-02T02:02:02"]
      }
    ]
  }
}

sink {
  GraphQL {
    plugin_input = "fake"
    url = "http://graphql:8080/v1/graphql"
    query = """
      mutation MyMutation(
        $id: Int!
        $val_bool: Boolean!
        $val_int8: smallint!
        $val_int16: smallint!
        $val_int32: Int!
        $val_int64: bigint!
        $val_float: Float!
        $val_double: Float!
        $val_decimal: numeric!
        $val_string: String!
        $val_unixtime_micros: timestamp!
      ) {
        insert_sink(objects: {
          id: $id,
          val_bool: $val_bool,
          val_int8: $val_int8,
          val_int16: $val_int16,
          val_int32: $val_int32,
          val_int64: $val_int64,
          val_float: $val_float,
          val_double: $val_double,
          val_decimal: $val_decimal,
          val_string: $val_string,
          val_unixtime_micros: $val_unixtime_micros
        }) {
          affected_rows
        }
      }
    """
  }
}
```

### Keep a Configured Variable Value

```hocon
sink {
  GraphQL {
    plugin_input = "fake"
    url = "http://graphql:8080/v1/graphql"
    valueCover = true
    variables = {
      val_bool = true
    }
    query = """
      mutation MyMutation($id: Int!, $val_bool: Boolean!) {
        insert_sink(objects: {id: $id, val_bool: $val_bool}) {
          affected_rows
        }
      }
    """
  }
}
```

### Stream Mutations to a GraphQL Service

Run the same mutation continuously in streaming mode. The sink sends one mutation
per row and uses `retry` plus exponential backoff to absorb transient HTTP
failures.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    plugin_output = "events"
    schema = {
      fields {
        id = int
        val_string = string
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "first"] }
      { kind = INSERT, fields = [2, "second"] }
    ]
  }
}

sink {
  GraphQL {
    plugin_input = "events"
    url = "http://graphql:8080/v1/graphql"
    headers = {
      Authorization = "Bearer ${secret}"
    }
    query = """
      mutation MyMutation($id: Int!, $val_string: String!) {
        insert_event(objects: {id: $id, val_string: $val_string}) {
          affected_rows
        }
      }
    """
    retry = 5
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 5000
  }
}
```

### Write Multiple Upstream Tables

```hocon
source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema = {
          table = "graphql_sink_1"
          fields {
            id = int
            val_bool = boolean
            val_string = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, true, "NEW"]
          }
        ]
      },
      {
        schema = {
          table = "graphql_sink_2"
          fields {
            id = int
            val_bool = boolean
            val_string = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [2, true, "READY"]
          }
        ]
      }
    ]
  }
}

sink {
  GraphQL {
    plugin_input = "fake"
    url = "http://graphql:8080/v1/graphql"
    query = """
      mutation MyMutation($id: Int!, $val_bool: Boolean!, $val_string: String!) {
        insert_sink(objects: {id: $id, val_bool: $val_bool, val_string: $val_string}) {
          affected_rows
        }
      }
    """
  }
}
```

## Changelog

<ChangeLog />
