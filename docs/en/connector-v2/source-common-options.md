---
sidebar_position: 3
---

# Source Common Options

> Common parameters of source connectors

:::caution warn

The old configuration name `result_table_name` is deprecated, please migrate to the new name `plugin_output` as soon as possible.

:::

| Name          | Type   | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|---------------|--------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| plugin_output | String | No       | -       | When `plugin_output` is not specified, the data processed by this plugin will not be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` <br/>When `plugin_output` is specified, the data processed by this plugin will be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` . The data set `(dataStream/dataset)` registered here can be directly accessed by other plugins by specifying `plugin_input` . |
| parallelism   | Int    | No       | -       | When `parallelism` is not specified, the `parallelism` in env is used by default. <br/>When parallelism is specified, it will override the parallelism in env.                                                                                                                                                                                                                                                                                                                                                                                                                    |

# Important note

When the job configuration `plugin_output` you must set the `plugin_input` parameter

## Task Example

### Simple:

> This registers a stream or batch data source and returns the table name `fake_table` at registration

```bash
source {
    FakeSourceStream {
        plugin_output = "fake_table"
    }
}
```

### Multiple Pipeline Simple

> This is to convert the data source fake and write it to two different sinks

```bash
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        c_timestamp = "timestamp"
        c_date = "date"
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_decimal = "decimal(30, 8)"
        c_row = {
          c_row = {
            c_int = int
          }
        }
      }
    }
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    # the query table name must same as field 'plugin_input'
    query = "select id, regexp_replace(name, '.+', 'b') as name, age+1 as age, pi() as pi, c_timestamp, c_date, c_map, c_array, c_decimal, c_row from dual"
  }
  # The SQL transform support base function and criteria operation
  # But the complex SQL unsupported yet, include: multi source table/rows JOIN and AGGREGATE operation and the like
}

sink {
  Console {
    plugin_input = "fake1"
  }
   Console {
    plugin_input = "fake"
  }
}
```

