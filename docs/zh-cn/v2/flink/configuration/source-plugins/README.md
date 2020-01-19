## Source(数据源) 插件的配置

### Source插件通用参数
| name | type | required | default value |
| --- | --- | --- | --- |
| [result_table_name](#result_table_name-string) | string | no | - |
| [field_name](#field_name-string) | string | no | - |


##### result_table_name [string]

不指定 `result_table_name时` ，此插件处理后的数据，不会被注册为一个可供其他插件直接访问的数据集(dataStream/dataset)，或者被称为临时表(table);

指定 `result_table_name` 时，此插件处理后的数据，会被注册为一个可供其他插件直接访问的数据集(dataStream/dataset)，或者被称为临时表(table)。此处注册的数据集(dataStream/dataset)，其他插件可通过指定 `source_table_name` 来直接访问。


#### field_name [string]

当从上级插件获取到数据时，可以指定获取到字段的名称，方便在后续的sql插件使用。

### 使用样例

```
source {
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }
}
```

> 数据源 `FakeSourceStream` 的结果将注册为名为 `fake` 的临时表。这个临时表，可以被任意 `Transform` 或者 `Sink` 插件通过指定 `source_table_name` 使用。\
`field_name` 将临时表的两列分别命名为`name`和`age`。