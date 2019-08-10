# Input 插件

### Input插件通用参数

| name | type | required | default value |
| --- | --- | --- | --- |
| [result_table_name](#result_table_name-string) | string | yes | - |
| [table_name](#table_name-string) | string | no | - |


##### result_table_name [string]

不指定 `result_table_name时` ，此插件处理后的数据，不会被注册为一个可供其他插件直接访问的数据集(dataset)，或者被称为临时表(table);

指定 `result_table_name` 时，此插件处理后的数据，会被注册为一个可供其他插件直接访问的数据集(dataset)，或者被称为临时表(table)。此处注册的数据集(dataset)，其他插件可通过指定 `source_table_name` 来直接访问。


##### table_name [string]

**\[从v1.4开始废弃\]** 功能同 `result_table_name`，后续 Release 版本中将删除此参数，建议使用 `result_table_name` 参数


### 使用样例

```
fake {
    result_table_name = "view_table_2"
}
```

> 数据源 `fake` 的结果将注册为名为 `view_table_2` 的临时表。这个临时表，可以被任意 `Filter` 或者 `Output` 插件通过指定 `source_table_name` 使用。