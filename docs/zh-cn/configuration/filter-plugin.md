# Filter 插件

### 共有变量

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |
| [result_table_name](#result_table_name-string) | string | no | - |


##### source_table_name [string]

指定 `Filter` 插件数据来源临时表表名，若为空则数据源为上一个 `Filter` 的处理结果

##### result_table_name [string]

指定 `Filter` 插件处理结果注册的临时表表名，若为空则不注册临时表。

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