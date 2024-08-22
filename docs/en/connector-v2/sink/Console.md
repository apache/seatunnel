# Console

> Console sink connector

## Support Connector Version

- All versions

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to send data to Console. Both support streaming and batch mode.

> For example, if the data from upstream is [`age: 12, name: jared`], the content send to console is the following: `{"name":"jared","age":17}`

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|        Name        |  Type   | Required | Default |                                                 Description                                                 |
|--------------------|---------|----------|---------|-------------------------------------------------------------------------------------------------------------|
| common-options     |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details |
| log.print.data     | boolean | No       | -       | Flag to determine whether data should be printed in the logs. The default value is `true`                   |
| log.print.delay.ms | int     | No       | -       | Delay in milliseconds between printing each data item to the logs. The default value is `0`.                |

## Task Example

### Simple:

> This is a randomly generated data, written to the console, with a degree of parallelism of 1

```
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    result_table_name = "fake"
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
    source_table_name = "fake"
  }
}
```

### Multiple Sources Simple:

> This is a multiple source and you can specify a data source to write to the specified end

```
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    result_table_name = "fake1"
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
    result_table_name = "fake2"
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
    source_table_name = "fake1"
  }
  Console {
    source_table_name = "fake2"
  }
}
```

## Console Sample Data

This is a printout from our console

```
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

