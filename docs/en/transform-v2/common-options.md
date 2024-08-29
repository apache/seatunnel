---
sidebar_position: 1
---

# Transform Common Options

> This is a process of intermediate conversion between the source and sink terminals,You can use sql statements to smoothly complete the conversion process

|       Name        |  Type  | Required | Default |                                                                                                                                                                                                                                                              Description                                                                                                                                                                                                                                                              |
|-------------------|--------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| result_table_name | String | No       | -       | When `source_table_name` is not specified, the current plugin processes the data set `(dataset)` output by the previous plugin in the configuration file; <br/>When `source_table_name` is specified, the current plugin is processing the data set corresponding to this parameter.                                                                                                                                                                                                                                                  |
| source_table_name | String | No       | -       | When `result_table_name` is not specified, the data processed by this plugin will not be registered as a data set that can be directly accessed by other plugins, or called a temporary table `(table)`; <br/>When `result_table_name` is specified, the data processed by this plugin will be registered as a data set `(dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` . The dataset registered here can be directly accessed by other plugins by specifying `source_table_name` . |

## Task Example

### Simple:

> This is the process of converting the data source to fake and write it to two different sinks, Detailed reference `transform`

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

