# 复制

> Copy 转换插件

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

Copy 转换通过复制已有字段的值来增加新字段。输出中仍保留原字段。当下游需要使用另一个字段名，同时又需要保留原字段时，可以使用该转换。在多表作业中，可以设置外层规则，并为指定表配置优先使用的单独规则。

## 选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| `fields` | `Map<String, String>` | 条件必填 | - | 将新字段名映射到已有的源字段名。 |
| `src_field` | `String` | 条件必填 | - | 已废弃的源字段选项，需与 `dest_field` 配套使用。 |
| `dest_field` | `String` | 条件必填 | - | 已废弃的目标字段选项，使用 `src_field` 时必填。 |
| `table_transform` | `List<Map<String, Object>>` | 否 | `[]` | 为指定表配置完整的 Copy 规则。 |
| `table_transform.table_path` | `String` | 每条规则中必填 | - | 该条规则选择的表路径，精确匹配。 |
| `table_match_regex` | `String` | 否 | `.*` | 选择应用外层规则的表路径的正则表达式。 |
| `rule_match_mode` | `String` | 否 | `FIRST_MATCH` | 控制同一表匹配多条规则时的处理方式。 |
| 通用选项 | - | 否 | - | 参见[转换插件通用选项](common-options/common-options.md)。 |

### fields [Map<String, String>]

每一项将**新字段名**映射到**已有源字段名**。例如，`name_copy = name` 会增加一个值来自 `name` 的新字段 `name_copy`。映射不能为空，源字段必须存在于输入表中。一条 Copy 规则必须使用 `fields`，或使用已废弃的 `src_field`、`dest_field` 组合；同一条规则不能混用两种写法。

```hocon
fields {
  name_copy = name
  age_copy = age
}
```

### src_field [String]

已废弃的单字段写法，必须与 `dest_field` 一起设置。新配置建议使用 `fields`。其值不能为空白，也不能在同一条规则中与 `fields` 同时使用。

### dest_field [String]

已废弃的目标字段选项。设置 `src_field` 时必须提供非空白的 `dest_field`。例如，`src_field = "name"` 和 `dest_field = "name_copy"` 会将 `name` 复制到 `name_copy`。

### table_transform [List<Map<String, Object>>]

每一项通过 `table_path` 选择一个表，并包含一条完整的 Copy 规则：非空的 `fields` 映射，或已废弃的 `src_field`、`dest_field` 组合。对同一个表，按表规则优先于外层规则。只配置 `table_transform` 时，未列出的表原样通过。参见[多表转换](transform-multi-table.md)。

### table_transform.table_path [String]

非空白的上游表路径，例如 `test.users`。它按表路径精确匹配，不是按 `plugin_output` 匹配。

### table_match_regex [String]

选择应用外层 Copy 规则的表路径的正则表达式。默认值为 `.*`，匹配所有表路径。即使某个表也符合该正则表达式，其对应的 `table_transform` 规则仍优先。

### rule_match_mode [String]

可选值为 `FIRST_MATCH` 和 `ALL_MATCH`，默认值为 `FIRST_MATCH`。`FIRST_MATCH` 使用该表匹配到的第一条规则；`ALL_MATCH` 按顺序执行所有匹配规则，因此后一条规则可以复制前一条规则创建的字段。未显式配置此选项时，重复的精确 `table_path` 会被拒绝。

### 通用选项

`plugin_input`、`plugin_output` 等共享设置参见[转换插件通用选项](common-options/common-options.md)。

## 多表配置

下面的完整作业生成两个表。`test.users` 的按表规则覆盖外层 `fields` 规则；`test.orders` 使用外层规则。即使表路径也符合 `table_match_regex`，对应的 `table_transform` 规则仍优先。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema = {
          table = "test.users"
          fields {
            id = int
            name = string
          }
        }
        rows = [
          { kind = INSERT, fields = [1, "Alice"] }
        ]
      },
      {
        schema = {
          table = "test.orders"
          fields {
            id = int
            name = string
          }
        }
        rows = [
          { kind = INSERT, fields = [2, "Bob"] }
        ]
      }
    ]
  }
}

transform {
  Copy {
    plugin_input = "fake"
    plugin_output = "copied"

    fields {
      name_copy = name
    }

    table_transform = [
      {
        table_path = "test.users"
        fields {
          user_name_copy = name
        }
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "copied"
  }
}
```

## 示例

对于上面的完整作业，输入和预期输出如下：

| 表 | 输入字段及值 | 输出字段及值 |
|----|--------------|--------------|
| `test.users` | `id=1, name=Alice` | `id=1, name=Alice, user_name_copy=Alice` |
| `test.orders` | `id=2, name=Bob` | `id=2, name=Bob, name_copy=Bob` |

可以基于上面的完整作业理解三种配置方式：

- **仅使用外层规则：**删除 `table_transform`，两个表都会增加 `name_copy`。
- **外层规则加按表规则：**使用上面的完整作业，`test.users` 增加 `user_name_copy`，`test.orders` 增加 `name_copy`。
- **仅使用按表规则：**删除外层 `fields`，`test.users` 增加 `user_name_copy`，`test.orders` 原样通过。

## 更新日志

### 新版本

- 新增 Copy 转换插件
- 支持将字段复制到新字段
