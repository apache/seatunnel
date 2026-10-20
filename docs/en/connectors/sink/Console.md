import ChangeLog from '../changelog/connector-console.md';

# Console

> Console sink connector

## Support Connector Version

- All versions

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to print upstream rows to the SeaTunnel task log. Console supports both batch and streaming jobs. It is mainly used for debugging, local verification, and examples, not for durable production storage.

For each non-empty row, Console prints the subtask index, row index, table id, row kind, and field values. Complex values such as arrays, maps, and nested rows are converted to readable strings before printing.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

Console can receive rows from multiple upstream tables and can apply schema change events for display. Because it writes to logs, it does not provide exactly-once delivery or CDC writes to an external system.

> The Console sink always enables schema-evolution handling (`ADD_COLUMN`, `DROP_COLUMN`, `RENAME_COLUMN`, `UPDATE_COLUMN`) so changes to the upstream schema are reflected in the printed row types.

## Options

| Name                     | Type    | Required | Default | Description                                                                                                  |
|--------------------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------|
| common-options           |         | No       | -       | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md) for details. |
| log.print.data           | boolean | No       | true    | Whether to print row data to the task log. Set it to `false` when you only want to keep the sink in the job graph without printing every row. |
| log.print.delay.ms       | int     | No       | 0       | Non-negative delay in milliseconds after each row is processed. It can slow down printing during debugging.   |
| multi_table_sink_replica | int     | No       | 1       | Writer replica count for each table in a multi-table sink job.                                                |

## Output Format

Console prints the row type once when the writer starts, and then prints each row in this format:

```text
subtaskIndex=<subtask>  rowIndex=<index>:  SeaTunnelRow#tableId=<table-id> SeaTunnelRow#kind=<row-kind> : <field1>, <field2>, ...
```

- `subtaskIndex` is the sink task that printed the row.
- `rowIndex` is counted inside each sink writer.
- `tableId` identifies the upstream table in multi-table jobs. Single-table jobs usually print `-1`.
- `row-kind` shows the row change type, such as `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, or `DELETE`.

## Task Example

### Simple

This example generates three rows and prints them to the task log.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 3
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "fake"
    log.print.data = true
    log.print.delay.ms = 0
  }
}
```

### Multiple Sources Simple

Use `plugin_input` to send different upstream datasets to different Console sinks.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake1"
    row.num = 3
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        sex = "string"
      }
    }
  }
   FakeSource {
    plugin_output = "fake2"
    row.num = 3
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
  Console {
    plugin_input = "fake2"
  }
}
```

### Multi-Table Input

When the upstream source produces multiple tables, Console can print all of them in one sink. The table id in the log helps distinguish which table produced each row.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        row.num = 2
        schema = {
          table = "test.table1"
          columns = [
            { name = id, type = bigint }
            { name = name, type = string }
          ]
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.table2"
          columns = [
            { name = id, type = bigint }
            { name = age, type = int }
          ]
        }
      }
    ]
  }
}

sink {
  Console {
    multi_table_sink_replica = 1
  }
}
```

## Console Sample Data

This is a printout from our console

```text
2022-12-19 11:01:45,417 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=1: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CpiOd, 8520946
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=2: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: eQqTs, 1256802974
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=3: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: UsRgO, 2053193072
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=4: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jDQJj, 1993016602
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=5: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: rqdKp, 1392682764
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=6: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: wCoWN, 986999925
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=7: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: qomTU, 72775247
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=8: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jcqXR, 1074529204
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=9: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: AkWIO, 1961723427
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=10: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: hBoib, 929089763
```

## Changelog

<ChangeLog />
