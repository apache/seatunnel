# Input 插件

### Input插件通用参数

| name | type | required | default value |
| --- | --- | --- | --- |
| [result_table_name](#result_table_name-string) | string | yes | - |
| [table_name](#table_name-string) | string | no | - |



##### result_table_name [string]

指定 `Input` 插件生成的数据注册的临时表表名，离线数据源必填，实时数据源选填。

##### table_name [string]

**\[已废弃\]** 功能同 `result_table_name`，后续 Release 版本中将删除此参数，建议使用 `result_table_name` 参数


### 使用样例

```
fake {
    result_table_name = "view_table_2"
}
```

> 数据源 `fake` 的结果将注册为名为 `view_table_2` 的临时表。这个临时表，可以被任意 `Filter` 或者 `Output` 插件使用。