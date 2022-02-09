# Source Plugin

## Source Plugin common parameters

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| result_table_name | string | no       | -             |
| field_name        | string | no       | -             |

### result_table_name [string]

When `result_table_name` is not specified, the data processed by this plugin will not be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` ;

When `result_table_name` is specified, the data processed by this plugin will be registered as a data set `(dataStream/dataset)` that can be directly accessed by other plugins, or called a temporary table `(table)` . The data set `(dataStream/dataset)` registered here can be directly accessed by other plugins by specifying `source_table_name` .

### field_name [string]

When the data is obtained from the upper-level plug-in, you can specify the name of the obtained field, which is convenient for use in subsequent sql plugins.

## Examples

```bash
source {
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }
}
```

> The result of the data source `FakeSourceStream` will be registered as a temporary table named `fake` . This temporary table can be used by any `Transform` or `Sink` plugin by specifying `source_table_name` .
>
> `field_name` names the two columns of the temporary table `name` and `age` respectively.
