# Source Common Options

> 数据源连接器公共参数

|       name        |  type  | required | default value |
|-------------------|--------|----------|---------------|
| result_table_name | string | no       | -             |
| parallelism       | int    | no       | -             |

### result_table_name [string]

当`result_table_name`没有被指定时，通过这个插件处理的数据将不会被注册为可以被其他插件直接访问的数据集`(dataStream/dataset)`，也不会被称作临时表`(table)`；

当指定了`result_table_name`时，通过这个插件处理的数据将会被注册为可以被其他插件直接访问的数据集`(dataStream/dataset)`，或者称为临时表`(table)`。在这里注册的数据集`(dataStream/dataset)`可以通过指定`source_table_name`被其他插件直接访问。

### parallelism [int]

如果 `parallelism` 没有指定,  将默认使用env的 `parallelism`

如果 `parallelism` 没有指定,  将覆盖env的 `parallelism`

## Example

```bash
source {
    FakeSourceStream {
        result_table_name = "fake"
    }
}
```

> 数据源`FakeSourceStream`的结果将被注册为一个名为`fake`的临时表。任何`Transform`或`Sink`插件都可以通过指定`source_table_name`来使用这个临时表。

