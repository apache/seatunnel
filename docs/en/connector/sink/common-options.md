# Common Options

> Common parameters of sink connectors

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| source_table_name | string | no       | -             |

### source_table_name [string]

When `source_table_name` is not specified, the current plug-in processes the data set `dataset` output by the previous plugin in the configuration file;

When `source_table_name` is specified, the current plug-in is processing the data set corresponding to this parameter.

## Examples

```bash
source {
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }
}

transform {
    sql {
      source_table_name = "fake"
      sql = "select name from fake"
      result_table_name = "fake_name"
    }
    sql {
      source_table_name = "fake"
      sql = "select age from fake"
      result_table_name = "fake_age"
    }
}

sink {
    console {
      source_table_name = "fake_name"
    }
}
```

> If `source_table_name` is not specified, the console outputs the data of the last transform, and if it is set to `fake_name` , it will output the data of `fake_name`
