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
2022-12-16 23:46:16,713 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - fields: name, age
2022-12-16 23:46:16,714 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - types: STRING, INT
2022-12-16 23:46:17,753 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=1 : XRMxN, 1011284614
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=2 : pWrpO, 843127905
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=3 : REBSo, 833504132
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=4 : WsFRN, 26587682
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=5 : vTuoq, 414594867
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=6 : CskBS, 1855664254
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=7 : HHYfL, 627284161
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=8 : twFct, 964724571
2022-12-16 23:46:17,754 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0: row=9 : JGLqE, 1919562939
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Console Sink Connector

### 2.3.0-beta 2022-10-20
- [Improve] Console sink support print subtask index ([3000](https://github.com/apache/incubator-seatunnel/pull/3000))
