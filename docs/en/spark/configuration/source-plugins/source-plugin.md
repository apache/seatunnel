# Source Plugin

## Source Plugin common parameters

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| result_table_name | string | yes      | -             |
| table_name        | string | no       | -             |

### result_table_name [string]

When `result_table_name` is not specified, the data processed by this plug-in will not be registered as a data set that can be directly accessed by other plugins, or called a temporary table (table);

When `result_table_name` is specified, the data processed by this plug-in will be registered as a data set (dataset) that can be directly accessed by other plug-ins, or called a temporary table (table). The dataset registered here can be directly accessed by other plugins by specifying `source_table_name`.

### table_name [string]

[Deprecated since v1.4] The function is the same as `result_table_name` , this parameter will be deleted in subsequent Release versions, and `result_table_name`  parameter is recommended.

## Example

```bash
fake {
    result_table_name = "view_table"
}
```

> The result of the data source `fake` will be registered as a temporary table named `view_table` . This temporary table can be used by any Filter or Output plugin by specifying `source_table_name` .
