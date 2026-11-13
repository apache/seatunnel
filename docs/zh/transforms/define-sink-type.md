# 定义写入字段类型

> DefineSinkType：为 sink 建表或写入阶段显式指定字段类型

## 描述

DefineSinkType 转换插件用于定义 sink 字段的目标存储类型，适用于开启 `savemode` 自动建表的场景。

## 参数

| 参数名  | 类型                      | 是否必填 | 默认值 | 说明 |
|:------:|---------------------------|----------|--------|------|
| columns | list<map<string, string>> | 是       |        | 需要定义的列，必须为每一列指定名称和类型 |
| table_match_regex | string | 否 | `.*` | 与上游表路径进行匹配的正则表达式，转换级别规则只应用于匹配的表。 |
| table_transform | array | 否 | - | 针对单表的规则，参见 [多表转换](transform-multi-table.md)。每个条目包含 `table_path` 以及该表的 `columns`。 |
| rule_match_mode | string | 否 | `FIRST_MATCH` | 当多个 `table_transform` 条目的 `table_path` 相同时，决定它们的匹配方式。 |

## 示例

### 指定部分字段的建表类型

```
transform {
  DefineSinkType {
    columns = [
        {
            column = "c1"
            type = "nvarchar2(10)"
        }
        {
            column = "c2"
            type = "datetime(6)"
        }
        {
            column = "c3"
            type = "your target type"
        }
    ]
  }
}
```
