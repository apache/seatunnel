# TableFilter

> TableFilter transform plugin

## Description

表过滤 transform，用于正向或者反向过滤部分表

## Options

|       name       | type   | required | default value | Description                                            |
|:----------------:|--------|----------|---------------|--------------------------------------------------------|
| database_pattern | string | no       |               | 指定数据库过滤模式，默认值为 null，表示不过滤。如果要过滤数据库名称，请将其设置为正则表达式。      |
|  schema_pattern  | string | no       |               | 指定 schema 过滤模式，默认值为 null，表示不过滤。如果要过滤架构名称，请将其设置为正则表达式。  |
|  table_pattern   | string | no       |               | 指定表过滤模式，默认值为 null，表示不过滤。如果要过滤表名称，请将其设置为正则表达式。          |
|   pattern_mode   | string | no       | INCLUDE       | 指定过滤模式，默认值为 INCLUDE，表示包含匹配的表。如果要排除匹配的表，请将其设置为 EXCLUDE。 |

## Examples

### 包含表过滤

在数据库 "test" 中包含名称与正则表达式 "user_\d+" 匹配的过滤表。

```hocon
transform {
    TableFilter {
        plugin_input = "source1"
        plugin_output = "transform_a_1"
    
        database_pattern = "test"
        table_pattern = "user_\\d+"
    }
}
```

### 排除表过滤

排除数据库 "test" 中名称与正则表达式 "user_\d+" 匹配的过滤表。

```hocon
transform {
    TableFilter {
        plugin_input = "source1"
        plugin_output = "transform_a_1"
    
        database_pattern = "test"
        table_pattern = "user_\\d+"
        pattern_mode = "EXCLUDE"
    }
}
```