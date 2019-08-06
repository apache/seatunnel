# Input 插件

### 共有变量

| name | type | required | default value |
| --- | --- | --- | --- |
| [result_table_name](#result_table_name-string) | string | yes | - |
| [table_name](#table_name-string) | string | no | - |



##### result_table_name [string]

指定 `Input` 插件生成的数据注册的临时表表名，离线数据源必填，实时数据源选填。

##### table_name [string]

功能同 `result_table_name`，已弃置，后续将停止使用。

### 使用样例

```
fake {
    result_table_name = "view_table_2"
}
```