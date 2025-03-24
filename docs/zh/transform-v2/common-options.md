# 转换常见选项

> 源端连接器的常见参数

:::caution 警告

旧的配置名称 `result_table_name`/`source_table_name` 已经过时，请尽快迁移到新名称 `plugin_output`/`plugin_input`。

:::

| 参数名称          | 参数类型   | 是否必须 | 默认值 |
|---------------|--------|------|-----|
| plugin_output | string | no   | -   |
| plugin_input  | string | no   | -   |

### plugin_input [string]

当未指定 `plugin_input` 时，当前插件在配置文件中处理由前一个插件输出的数据集 `(dataset)` ；

当指定了 `plugin_input` 时，当前插件正在处理与该参数对应的数据集

### plugin_output [string]

当未指定 `plugin_output` 时，此插件处理的数据不会被注册为其他插件可以直接访问的数据集，也不会被称为临时表 `(table)`；

当指定了 `plugin_output` 时，此插件处理的数据将被注册为其他插件可以直接访问的数据集 `(dataset)`，或者被称为临时表 `(table)`。在这里注册的数据集可以通过指定 `plugin_input` 被其他插件直接访问。

## 示例

