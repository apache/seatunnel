# Socket

> Socket sink connector

## Description

Used to send data to Socket Server. Both support streaming and batch mode.
> For example, if the data from upstream is [`age: 12, name: jared`], the content send to socket server is the following: `{"name":"jared","age":17}`

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name           | type   | required | default value |
| -------------- |--------|----------|---------------|
| host           | String | Yes      | -             |
| port           | Integer| yes      | -             |
| max_retries    | Integer| No       | 3             |
| common-options |        | no       | -             |

### host [string]
socket server host

### port [integer]

socket server port

### max_retries [integer]

The number of retries to send record failed

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

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

transform {
      sql = "select name, age from fake"
}

sink {
    Socket {
        host = "localhost"
        port = 9999
    }
}

```

* Start a port listening

```shell
nc -l -v 9999
```

* Start a SeaTunnel task


* Socket Server Console print data

```text
{"name":"jared","age":17}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Socket Sink Connector
