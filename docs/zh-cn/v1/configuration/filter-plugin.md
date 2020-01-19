# Filter 插件

### Filter插件通用参数

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |
| [result_table_name](#result_table_name-string) | string | no | - |


##### source_table_name [string]

不指定 `source_table_name` 时，当前插件处理的就是配置文件中上一个插件输出的数据集(dataset)；

指定 `source_table_name` 的时候，当前插件处理的就是此参数对应的数据集。

##### result_table_name [string]

不指定 `result_table_name时` ，此插件处理后的数据，不会被注册为一个可供其他插件直接访问的数据集(dataset)，或者被称为临时表(table);

指定 `result_table_name` 时，此插件处理后的数据，会被注册为一个可供其他插件直接访问的数据集(dataset)，或者被称为临时表(table)。此处注册的数据集(dataset)，其他插件可通过指定 `source_table_name` 来直接访问。

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

> `Split` 插件将会处理临时表 `view_table_1` 中的数据，并将处理结果注册为名为 `view_table_2` 的临时表， 这张临时表可以被后续任意 `Filter` 或 `Output` 插件通过指定 `source_table_name` 使用。

```
split {
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
}
```

> 没有配置 `source_table_name`，`Split` 插件会读取上一个插件传递过来的数据集，并且传递给下一个插件。