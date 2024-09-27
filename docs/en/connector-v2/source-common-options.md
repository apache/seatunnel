---
sidebar_position: 3
---

# Source Common Options

> Common parameters of source connectors

|       Name        |  Type  | Required | Default |                                                                                                                                                                                                                                                                                          Description                                                                                                                                                                                                                                                                                           |
|-------------------|--------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| result_table_name | String | No       | -       | When `result_table_name` is not specified, the data processed by this plugin will not be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` <br/>When `result_table_name` is specified, the data processed by this plugin will be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` . The data set `(dataStream/dataset)` registered here can be directly accessed by other plugins by specifying `source_table_name` . |
| parallelism       | Int    | No       | -       | When `parallelism` is not specified, the `parallelism` in env is used by default. <br/>When parallelism is specified, it will override the parallelism in env.                                                                                                                                                                                                                                                                                                                                                                                                                                 |

# Important note

When the job configuration `result_table_name` you must set the `source_table_name` parameter

## Task Example

### Simple:

> This registers a stream or batch data source and returns the table name `fake_table` at registration

```bash
source {
    FakeSourceStream {
        result_table_name = "fake_table"
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
    result_table_name = "fake"
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
    source_table_name = "fake"
    result_table_name = "fake1"
    # the query table name must same as field 'source_table_name'
    query = "select id, regexp_replace(name, '.+', 'b') as name, age+1 as age, pi() as pi, c_timestamp, c_date, c_map, c_array, c_decimal, c_row from fake"
  }
  # The SQL transform support base function and criteria operation
  # But the complex SQL unsupported yet, include: multi source table/rows JOIN and AGGREGATE operation and the like
}

sink {
  Console {
    source_table_name = "fake1"
  }
   Console {
    source_table_name = "fake"
  }
}
```

