# 表过滤

> TableFilter：按库名、schema 或表名规则筛选需要处理的表

## 描述

TableFilter 转换插件用于按表名、库名或 schema 规则，正向或反向过滤部分表。

## 参数

| 参数名            | 类型   | 是否必填 | 默认值 | 说明 |
|:----------------:|--------|----------|--------|------|
| database_pattern | string | 否       |        | 数据库过滤规则。默认不过滤；如需过滤数据库名称，请填写正则表达式。 |
| schema_pattern   | string | 否       |        | schema 过滤规则。默认不过滤；如需过滤 schema 名称，请填写正则表达式。 |
| table_pattern    | string | 否       |        | 表过滤规则。默认不过滤；如需过滤表名称，请填写正则表达式。 |
| pattern_mode     | enum   | 否       | INCLUDE | 过滤模式。`INCLUDE` 表示保留匹配的表，`EXCLUDE` 表示排除匹配的表。 |
| table_match_regex | string | 否       | `.*`    | 与上游表路径进行匹配的正则表达式，转换级别规则只应用于匹配的表。 |
| table_transform   | array  | 否       | -       | 针对单表的规则，参见 [多表转换](transform-multi-table.md)。每个条目包含 `table_path` 以及该表的过滤规则（上述选项）。 |
| rule_match_mode   | string | 否       | `FIRST_MATCH` | 当多个 `table_transform` 条目的 `table_path` 相同时，决定它们的匹配方式。 |

## 示例

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
