# Console

> Console sink connector

## Description

Used to send data to Console. Both support streaming and batch mode.
> For example, if the data from upstream is [`age: 12, name: jared`], the content send to console is the following: `{"name":"jared","age":17}`

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

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
row=1 : XTblOoJMBr, 1968671376
row=2 : NAoJoFrthI, 1603900622
row=3 : VHZBzqQAPr, 1713899051
row=4 : pfUYOOrPgA, 1412123956
row=5 : dCNFobURas, 202987936
row=6 : XGWVgFnfWA, 1879270917
row=7 : KIGOqnLhqe, 430165110
row=8 : goMdjHlRpX, 288221239
row=9 : VBtpiNGArV, 1906991577
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Console Sink Connector

### 2.3.0-beta 2022-10-20
- [Improve] Console sink support print subtask index ([3000](https://github.com/apache/incubator-seatunnel/pull/3000))
