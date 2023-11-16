# Socket

> Socket source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

> Used to read data from Socket.

## Source Options

|      Name      |  Type   | Required | Default |                                               Description                                               |
|----------------|---------|----------|---------|---------------------------------------------------------------------------------------------------------|
| host           | String  | Yes      |         | socket server host                                                                                      |
| port           | Integer | Yes      |         | socket server port                                                                                      |
| common-options |         | No       | -       | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details |

## Task Example

### Simple:

> This is a streaming accept Socket data source Writes to console output

```hocon
env {
  # You can set flink configuration here
  execution.parallelism = 1
}
Socket {
  host = "localhost"
  port = 9999
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
  Console {}
}
```

* Start a port listening

```shell
nc -l 9999
```

* Start a SeaTunnel task

* Socket Source send test data

```text
~ nc -l 9999
test
hello
flink
spark
```

* Console Sink print data

```text
[test]
[hello]
[flink]
[spark]
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Socket Source Connector

### Next Version

- `host` and `port` become required ([3317](https://github.com/apache/seatunnel/pull/3317))

