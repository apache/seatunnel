# Output 插件

### Output插件通用参数

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |



##### source_table_name [string]

指定 `Output` 插件数据来源临时表表名。如果没有指定该参数，默认输出的数据集(dataset)为配置文件中最后一个 `Filter` 插件处理后的数据集(dataset)


### 使用样例

```
stdout {
    source_table_name = "view_table_2"
}
```

> 将名为 `view_table_2` 的临时表输出。

```
stdout {}
```

> 若不配置`source_table_name`, 将配置文件中最后一个 `Filter` 插件的处理结果输出