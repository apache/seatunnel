import ChangeLog from '../changelog/connector-druid.md';

# Druid

> Druid sink connector

## Description

Write data to Apache Druid through the Druid indexing task API.

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

| SeaTunnel Data Type | Druid Data Type |
|---------------------|-----------------|
| TINYINT             | LONG            |
| SMALLINT            | LONG            |
| INT                 | LONG            |
| BIGINT              | LONG            |
| FLOAT               | FLOAT           |
| DOUBLE              | DOUBLE          |
| DECIMAL             | DOUBLE          |
| STRING              | STRING          |
| BOOLEAN             | STRING          |
| TIMESTAMP           | STRING          |

## Options

| name           | type   | required | default value | description |
|----------------|--------|----------|---------------|-------------|
| coordinatorUrl | string | yes      | -             | Druid coordinator or router host and port. |
| datasource     | string | yes      | -             | Druid datasource name. Supports placeholders such as `${table_name}`. |
| batchSize      | int    | no       | 10000         | Number of rows buffered before one indexing task is submitted. |
| common-options |        | no       | -             | Sink common options. |

### coordinatorUrl [string]

The Druid coordinator or router host and port, for example `router:8888`.

SeaTunnel sends indexing tasks to `http://{coordinatorUrl}/druid/indexer/v1/task`, so configure only the host and port. Do not include the protocol or API path.

### datasource [string]

The Druid datasource name to write.

When the upstream source has multiple tables, you can use placeholders such as `${table_name}` to route each upstream table to a different Druid datasource.

### batchSize [int]

The number of rows buffered before SeaTunnel sends one indexing task to Druid. The default value is `10000`.

SeaTunnel also flushes the remaining buffered rows when the writer closes.

Use a larger value for high-throughput batch jobs to reduce the number of Druid indexing tasks. Use a smaller value when you need data to be submitted to Druid earlier, but note that this can create more indexing tasks.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

For multi-table writes, `multi_table_sink_replica` can be used with the common sink options.

## Write Behavior

The connector converts each SeaTunnel row into inline CSV data and submits it to Druid as a native batch indexing task.

The connector adds a processing-time column named `timestamp` to satisfy Druid's primary timestamp requirement. This generated timestamp is used by Druid ingestion; source `TIMESTAMP` fields are written as string dimensions according to the mapping above.

Rows are flushed when the buffered row count reaches `batchSize`. Remaining rows are also flushed when the writer closes. There is no time-based periodic flush option.

The sink is designed for append-style batch ingestion. It does not interpret CDC update/delete row kinds as Druid upserts or deletes.

Only the data types listed in [Data Type Mapping](#data-type-mapping) are supported. Other SeaTunnel types fail during write planning.

Because the connector submits inline CSV data to Druid, string values should not contain raw commas or line breaks unless they are normalized before the Druid sink.

## Example

### Write One Table

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
        c_boolean = boolean
        c_timestamp = timestamp
        c_string = string
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [true, "2020-02-02T02:02:02", "NEW", 1, 2, 3, 4, 4.3, 5.3, 6.3]
      },
      {
        kind = INSERT
        fields = [false, "2012-12-21T12:34:56", "AAA", 1, 1, 333, 323232, 3.1, 9.33333, 99999.99999999]
      }
    ]
  }
}

sink {
  Druid {
    coordinatorUrl = "router:8888"
    datasource = "testDataSource"
    batchSize = 10000
  }
}
```

### Write Multiple Tables

Use `${table_name}` to write each upstream table to a Druid datasource with the same table name.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "druid_sink_1"
          fields {
            id = int
            val_bool = boolean
            val_tinyint = tinyint
            val_smallint = smallint
            val_int = int
            val_bigint = bigint
            val_float = float
            val_double = double
            val_decimal = "decimal(16, 1)"
            val_string = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, true, 1, 2, 3, 4, 4.3, 5.3, 6.3, "NEW"]
          }
        ]
      },
      {
        schema = {
          table = "druid_sink_2"
          fields {
            id = int
            val_bool = boolean
            val_tinyint = tinyint
            val_smallint = smallint
            val_int = int
            val_bigint = bigint
            val_float = float
            val_double = double
            val_decimal = "decimal(16, 1)"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, true, 1, 2, 3, 4, 4.3, 5.3, 6.3]
          }
        ]
      }
    ]
  }
}

sink {
  Druid {
    coordinatorUrl = "router:8888"
    datasource = "${table_name}"
  }
}
```

## Changelog

<ChangeLog />
