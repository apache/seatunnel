# Output 插件

### Output插件通用参数

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_table_name](#source_table_name-string) | string | no | - |



##### source_table_name [string]

不指定 `source_table_name` 时，当前插件处理的就是配置文件中上一个插件输出的数据集(dataset)；

指定 `source_table_name` 的时候，当前插件处理的就是此参数对应的数据集。


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