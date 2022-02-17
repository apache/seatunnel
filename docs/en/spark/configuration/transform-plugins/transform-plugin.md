# Common Options

> Transform Common Options [Spark]

## Transform Plugin common parameters

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| source_table_name | string | no       | -             |
| result_table_name | string | no       | -             |

### source_table_name [string]

When `source_table_name` is not specified, the current plug-in processes the data set `(dataset)` output by the previous plug-in in the configuration file;

When `source_table_name` is specified, the current plugin is processing the data set corresponding to this parameter.

### result_table_name [string]

When `result_table_name` is not specified, the data processed by this plugin will not be registered as a data set that can be directly accessed by other plugins, or called a temporary table `(table)`;

When `result_table_name` is specified, the data processed by this plugin will be registered as a data set `(dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` . The dataset registered here can be directly accessed by other plugins by specifying `source_table_name` .

## Examples

```bash
split {
    source_table_name = "source_view_table"
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
    result_table_name = "result_view_table"
}
```

> The `Split` plugin will process the data in the temporary table `source_view_table` and register the processing result as a temporary table named `result_view_table`. This temporary table can be used by any subsequent `Filter` or `Output` plugin by specifying `source_table_name` .

```bash
split {
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
}
```

> If `source_table_name` is not configured, output the processing result of the last `Transform` plugin in the configuration file
