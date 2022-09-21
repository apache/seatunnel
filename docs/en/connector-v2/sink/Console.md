# Console

> Console sink connector

## Description

Used to send data to Console. Both support streaming and batch mode.
> For example, if the data from upstream is [`age: 12, name: jared`], the content send to console is the following: `{"name":"jared","age":17}`

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name | type   | required | default value |
| --- |--------|----------|---------------|
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

transform {
      sql {
        sql = "select name, age from fake"
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
{"name":"jared","age":17}
```
