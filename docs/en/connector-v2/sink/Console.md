# Console

> Console sink connector

## Description

Used to send data to Console. Both support streaming and batch mode.
> For example, if the data from upstream is [`age: 12, name: jared`], the content send to console is the following: `{"name":"jared","age":17}`

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

##  Options

| name            | type  | required | default value |
| -------------  |--------|----------|---------------|
| common-options |        | no       | -             |

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

simple:

```hocon
Console {

    }
```

test:

* Configuring the SeaTunnel config file

```hocon
env {
  execution.parallelism = 1
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

    }
}

```

* Start a SeaTunnel task


* Console print data

```text
2022-12-19 11:01:45,417 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - fields info: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=1 : CpiOd, 8520946
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=2 : eQqTs, 1256802974
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=3 : UsRgO, 2053193072
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=4 : jDQJj, 1993016602
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=5 : rqdKp, 1392682764
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=6 : wCoWN, 986999925
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=7 : qomTU, 72775247
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=8 : jcqXR, 1074529204
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=9 : AkWIO, 1961723427
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT row=10 : hBoib, 929089763
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Console Sink Connector

### 2.3.0-beta 2022-10-20
- [Improve] Console sink support print subtask index ([3000](https://github.com/apache/incubator-seatunnel/pull/3000))
