# 替换

> 替换转换插件

## 描述

检查给定字段中的字符串值，并用给定的替换项替换与给定字符串字面量或正则表达式匹配的字符串值的子字符串。

## 属性

|      名称       | 类型      | 是否必须 |  默认值  |
|---------------|---------|------|-------|
| replace_fields | array   | yes  |       |
| pattern       | string  | yes  | -     |
| replacement   | string  | yes  | -     |
| is_regex      | boolean | no   | false |
| replace_first | boolean | no   | false |
| table_match_regex | string  | no   | `.*`  |
| table_transform | array   | no   | -     |
| rule_match_mode | string  | no   | `FIRST_MATCH` |

### replace_fields [array]

需要替换的字段

`replace_fields` 支持列表形式，例如：`["name"]` 或 `["name", "title"]`。

为了向后兼容，旧配置 `replace_field = "name"` 仍然支持，但新配置建议统一使用 `replace_fields`。

输入模式刷新时（包括从检查点恢复模式），会重新解析配置的字段名。因此，添加、删除或重排其他列不会改变需要执行替换的字段。如果配置的字段在刷新后的模式中不存在（例如已被删除或重命名），刷新会在处理后续数据行之前报出字段不存在错误。重新启动作业之前，请更新 `replace_fields`，使其与源表模式一致。

此字段绑定行为不会改变引擎对模式演变的支持，也不会改变 Replace 现有的值转换和输出模式行为。

### pattern [string]

将被替换的旧字符串

### replacement [string]

用于替换的新字符串

### is_regex [boolean]

使用正则表达式进行字符串匹配

### replace_first [boolean]

是否替换第一个匹配字符串。仅在 `is_regex = true` 时使用。

### table_match_regex [string]

与上游表路径进行匹配的正则表达式。转换级别规则（位于 `table_transform` 之外的上述选项）只会应用于路径匹配该正则表达式的表。默认值 `.*` 匹配所有表。

### table_transform [array]

针对单表的规则列表。每个条目必须包含 `table_path`（格式为 `databaseName[.schemaName].tableName`，精确匹配），以及该表的一条完整的 Replace 规则（`replace_fields`、`pattern`、`replacement`）。有对应条目的表不会再受转换级别规则影响。

### rule_match_mode [string]

控制当多个 `table_transform` 条目的 `table_path` 相同时的处理方式：`FIRST_MATCH` 只应用第一个匹配的条目，`ALL_MATCH` 按顺序应用所有匹配的条目。如果未设置，重复的 `table_path` 条目会被拒绝。

### common options [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options/common-options.md) 了解详情

## 示例

源端数据读取的表格如下：

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

我们想要将 `name` 字段中的字符 ` `(空格)替换为 `_`。然后我们可以添加一个 `Replace` 转换，像这样：

```
transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_fields = ["name"]
    pattern = " "
    replacement = "_"
    is_regex = true
  }
}
```

那么结果表 `fake1` 中的数据将会更新为：

|   name   | age | card |
|----------|-----|------|
| Joy_Ding | 20  | 123  |
| May_Ding | 20  | 123  |
| Kin_Dom  | 20  | 123  |
| Joy_Dom  | 20  | 123  |

## 作业配置示例

```
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
      }
    }
  }
}

transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_fields = ["name"]
    pattern = ".+"
    replacement = "b"
    is_regex = true
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## 更新日志

### 新版本

- 添加替换转换连接器
