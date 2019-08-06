# Output 插件

### 共有变量

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |



##### source_table_name [string]

指定 `Output` 插件数据来源临时表表名，若为空，则读取最后一个 `Filter` 处理后的结果


### 使用样例

```
stdout {
    source_table_name = "view_table_2"
}
```