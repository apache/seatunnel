# Filter 插件

### Filter插件通用参数

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |
| [result_table_name](#result_table_name-string) | string | no | - |


##### source_table_name [string]

指定 `Filter` 插件数据来源临时表表名，如果没有配置该参数，则数据源为上一个 `Filter` 的处理结果

##### result_table_name [string]

指定 `Filter` 插件处理结果注册的临时表表名，如果没有配置该参数，则不注册临时表。

### 使用样例

```
split {
    source_table_name = "view_table_1"
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
    result_table_name = "view_table_2"
}
```

> `Split` 插件将会处理临时表 `view_table_1` 中的数据，并将处理结果注册为名为 `view_table_2` 的临时表， 这张临时表可以被后续任意 `Filter` 或 `Output` 插件使用。

```
split {
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
}
```

> 没有配置 `source_table_name`，`Split` 插件会读取上一个插件传递过来的数据集，并且传递给下一个插件。