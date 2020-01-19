# Source Plugin

### Source Common Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [result_table_name](#result_table_name-string) | string | yes | - |

##### result_table_name [string]

不指定 `result_table_name`时 ，此插件处理后的数据，不会被注册为一个可供其他插件直接访问的数据集(dataset)，或者被称为临时表(table);

指定 `result_table_name` 时，此插件处理后的数据，会被注册为一个可供其他插件直接访问的数据集(dataset)，或者被称为临时表(table)。此处注册的数据集(dataset)，其他插件可通过指定 `source_table_name` 来直接访问。


### Examples

```
fake {
    result_table_name = "view_table_2"
}
```
