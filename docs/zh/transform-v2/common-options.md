# 转换常见选项

> 源端连接器的常见参数

|       参数名称        |  参数类型  | 是否必须 | 默认值 |
|-------------------|--------|------|-----|
| result_table_name | string | no   | -   |
| source_table_name | string | no   | -   |

### source_table_name [string]

当未指定 `source_table_name` 时，当前插件在配置文件中处理由前一个插件输出的数据集 `(dataset)` ；

当指定了 `source_table_name` 时，当前插件正在处理与该参数对应的数据集

### result_table_name [string]

当未指定 `result_table_name` 时，此插件处理的数据不会被注册为其他插件可以直接访问的数据集，也不会被称为临时表 `(table)`；

当指定了 `result_table_name` 时，此插件处理的数据将被注册为其他插件可以直接访问的数据集 `(dataset)`，或者被称为临时表 `(table)`。在这里注册的数据集可以通过指定 `source_table_name` 被其他插件直接访问。

## 示例

