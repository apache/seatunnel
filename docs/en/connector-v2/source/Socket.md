# Socket

> Socket source connector

## Description

Used to read data from Socket.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

##  Options

| name           | type   | required | default value |
| -------------- |--------| -------- | ------------- |
| host           | String | No       | localhost     |
| port           | Integer| No       | 9999          |
| common-options |        | no       | -             |

### host [string]
socket server host

### port [integer]

socket server port

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example

simple:

```hocon
Socket {
        host = "localhost"
        port = 9999
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
    Socket {
        host = "localhost"
        port = 9999
    }
}

transform {
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
