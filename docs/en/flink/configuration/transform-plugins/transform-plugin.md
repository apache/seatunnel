# Common Options

> Transform Common Options [Flink]

## Transform Plugin common parameters

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| source_table_name | string | no       | -             |
| result_table_name | string | no       | -             |
| field_name        | string | no       | -             |

### source_table_name [string]

When `source_table_name` is not specified, the current plugin is processing the data set `(dataStream/dataset)` output by the previous plugin in the configuration file;

When `source_table_name` is specified, the current plugin is processing the data set corresponding to this parameter.

### result_table_name [string]

When `result_table_name` is not specified, the data processed by this plugin will not be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table (table);

When `result_table_name` is specified, the data processed by this plugin will be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` . The data set `(dataStream/dataset)` registered here can be directly accessed by other plugins by specifying `source_table_name` .

### field_name [string]

When the data is obtained from the upper-level plugin, you can specify the name of the obtained field, which is convenient for use in subsequent sql plugins.

## Examples

```bash
source {
    FakeSourceStream {
      result_table_name = "fake_1"
      field_name = "name,age"
    }
    FakeSourceStream {
      result_table_name = "fake_2"
      field_name = "name,age"
    }
}

transform {
    sql {
      source_table_name = "fake_1"
      sql = "select name from fake_1"
      result_table_name = "fake_name"
    }
}
```

> If `source_table_name` is not specified, the sql plugin will process the data of `fake_2` , and if it is set to `fake_1` , it will process the data of `fake_1` .
