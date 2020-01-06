## Sink(数据输出) 插件的配置

### Sink插件通用参数
| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |


##### source_table_name [string]

不指定 `source_table_name` 时，当前插件处理的就是配置文件中上一个插件输出的数据集(dataStream/dataset)；

指定 `source_table_name` 的时候，当前插件处理的就是此参数对应的数据集。


### 使用样例

```
source {
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }
}

transform {
    sql {
      source_table_name = "fake"
      sql = "select name from fake"
      result_table_name = "fake_name"
    }
    sql {
      source_table_name = "fake"
      sql = "select age from fake"
      result_table_name = "fake_age"
    }
}

sink {
    console {
      source_table_name = "fake_name"
    }
}
```

> 如果不指定`source_table_name`的话，console输出的是最后一个transform的数据，设置了为`fake_name`将输出`fake_name`的数据