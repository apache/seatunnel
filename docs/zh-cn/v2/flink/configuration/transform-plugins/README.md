## Transform(数据转换) 插件的配置

### Transform插件通用参数
| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |
| [result_table_name](#result_table_name-string) | string | no | - |
| [field_name](#field_name-string) | string | no | - |


##### source_table_name [string]

不指定 `source_table_name` 时，当前插件处理的就是配置文件中上一个插件输出的数据集(dataStream/dataset)；

指定 `source_table_name` 的时候，当前插件处理的就是此参数对应的数据集。

##### result_table_name [string]

不指定 `result_table_name时` ，此插件处理后的数据，不会被注册为一个可供其他插件直接访问的数据集(dataStream/dataset)，或者被称为临时表(table);

指定 `result_table_name` 时，此插件处理后的数据，会被注册为一个可供其他插件直接访问的数据集(dataStream/dataset)，或者被称为临时表(table)。此处注册的数据集(dataStream/dataset)，其他插件可通过指定 `source_table_name` 来直接访问。


#### field_name [string]

当从上级插件获取到数据时，可以指定获取到字段的名称，方便在后续的sql插件使用。

### 使用样例

```
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

> 如果不指定`source_table_name`的话，sql插件处理的就是 `fake_2`的数据，设置了为`fake_1`将处理`fake_1`的数据