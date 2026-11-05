# 过滤器

> 过滤器转换插件

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 描述

Filter 转换用于决定表中的哪些字段会传递给下一步。使用 `include_fields` 列出需要保留的字段，或使用 `exclude_fields` 列出需要删除的字段，二者必须设置其一。表名不会改变；只有当主键和约束键的所有列都仍然保留在输出中时，主键和约束键才会被保留。Filter 支持多表配置，参见 [多表配置](#多表配置)。

## 主要特性

- 保留指定的字段列表（`include_fields`）或删除指定的字段列表（`exclude_fields`）
- 使用 `include_fields` 时，输出字段的顺序与列表中的顺序一致
- 当主键和约束键的列仍然保留时，主键和约束键会被保留
- 一个转换即可处理多张表，参见 [多表转换](transform-multi-table.md)

## 选项

| 名称                | 类型            | 是否必须 | 默认值   | 描述                                                       |
|-------------------|---------------|------|-------|----------------------------------------------------------|
| include_fields    | Array&lt;String&gt; | 否    | -     | 需要保留的字段，其余字段将被删除。与 `exclude_fields` 互斥。                 |
| exclude_fields    | Array&lt;String&gt; | 否    | -     | 需要删除的字段，其余字段将被保留。与 `include_fields` 互斥。                 |
| table_match_regex | String        | 否    | `.*`  | 转换级别规则所适用的表路径的正则表达式。                                     |
| table_transform   | Array&lt;Map&gt;    | 否    | -     | 针对单表的规则，参见 [多表转换](transform-multi-table.md)。             |
| rule_match_mode   | String        | 否    | `FIRST_MATCH` | 当多个 `table_transform` 条目的 `table_path` 相同时，决定它们的匹配方式。 |
| common options    | -             | 否    | -     | 转换插件的通用参数，参见 [Transform Plugin](common-options/common-options.md)。 |

:::caution

每条规则中，`include_fields` 和 `exclude_fields` 必须设置且只能设置其中一个。同时设置两个或都不设置，都属于配置错误。

:::

### include_fields [Array&lt;String&gt;]

需要保留的字段，例如 `include_fields = [name, card]` 表示只保留这两个字段，其余字段将被删除。

输出字段按照此列表中的顺序排列，而不是按照输入表中的顺序。每个名称都必须存在于输入表中，否则作业会因 "cannot find input fields" 错误而失败。如果设置了该选项，列表不能为空。该选项不能与 `exclude_fields` 同时使用。

:::info

为了向后兼容，已废弃的选项名 `fields` 仍然可以作为 `include_fields` 的替代被接受。新配置请使用 `include_fields`。

:::

### exclude_fields [Array&lt;String&gt;]

需要删除的字段，例如 `exclude_fields = [age]` 表示删除 `age`，保留其余所有字段。当你只想从字段很多的表中删除少数几个字段时，这个选项会很方便。

保留下来的字段维持原有顺序。每个名称都必须存在于输入表中，否则作业会因 "cannot find input fields" 错误而失败。如果设置了该选项，列表不能为空。该选项不能与 `include_fields` 同时使用。

### table_match_regex [String]

与上游表路径进行匹配的正则表达式。转换级别规则（位于 `table_transform` 之外的 `include_fields` 或 `exclude_fields`）只会应用于匹配的表。默认值 `.*` 匹配所有表。既不匹配该正则表达式、也没有 `table_transform` 条目的表，将保持不变地传递下去。

注意，这里匹配的是真实的上游表名，而不是 `plugin_output`。

### table_transform [Array&lt;Map&gt;]

针对单表的规则列表。每个条目必须包含 `table_path`（格式为 `databaseName[.schemaName].tableName`，精确匹配），以及该表的一条完整的 Filter 规则，即 `include_fields` 和 `exclude_fields` 中恰好设置一个。

当某张表在 `table_transform` 中有对应条目时，转换级别规则不会应用于该表。参见 [多表配置](#多表配置)。

### rule_match_mode [String]

控制当多个 `table_transform` 条目的 `table_path` 相同时的处理方式。支持的值为 `FIRST_MATCH`（只应用第一个匹配的条目）和 `ALL_MATCH`（按顺序应用所有匹配的条目，每个条目接收前一个条目的输出）。

如果未设置 `rule_match_mode`，重复的 `table_path` 条目会被拒绝。当多个条目使用相同的 `table_path` 时，请显式设置为 `FIRST_MATCH` 或 `ALL_MATCH`。

### common options

转换插件的通用参数，请参考 [Transform Plugin](common-options/common-options.md) 了解详情。

## 多表配置

当上游数据源输出多张表时（例如 JDBC 或 MySQL-CDC），一个 Filter 转换就可以处理所有这些表。

- `table_transform` 中的条目优先于转换级别规则。有对应条目的表不会再受转换级别规则影响。
- 每个条目必须是一条完整的单表规则（`table_path` 加上 `include_fields` 或 `exclude_fields`）。
- 转换级别规则只应用于匹配 `table_match_regex` 的表。

下面的配置对 test.a.* 表使用转换级别规则，对 `test.xyz` 使用单表规则，并保持 `test.www` 不变：

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
        row.num = 2
        schema = {
          table = "test.abc"
          fields { id = int, name = string, age = int, card = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.abcd"
          fields { id = int, name = string, age = int, card = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.xyz"
          fields { id = int, name = string, age = int, card = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.www"
          fields { id = int, name = string, age = int, card = int }
        }
      }
    ]
  }
}

transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    include_fields = [name, card]

    table_transform = [
      {
        table_path = "test.xyz"
        exclude_fields = [age]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

每张表的结果结构：

| 表         | 应用的规则                          | 输出字段                |
|-----------|--------------------------------|---------------------|
| test.abc  | 转换级别规则（匹配 `test.a.*`）          | name, card          |
| test.abcd | 转换级别规则（匹配 `test.a.*`）          | name, card          |
| test.xyz  | `table_transform` 条目           | id, name, card      |
| test.www  | 无                              | id, name, age, card |

## 示例

### 单表

源端读取的数据是这样的一张表：

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

如果想保留字段 `name` 和 `card`，可以添加一个使用 `include_fields` 的 `Filter` 转换：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields { name = string, age = int, card = int }
    }
    rows = [
      { kind = INSERT, fields = ["Joy Ding", 20, 123] },
      { kind = INSERT, fields = ["May Ding", 20, 123] },
      { kind = INSERT, fields = ["Kin Dom", 20, 123] },
      { kind = INSERT, fields = ["Joy Dom", 20, 123] }
    ]
  }
}

transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    include_fields = [name, card]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

也可以使用 `exclude_fields` 删除字段 `age`，得到的结果相同：

```hocon
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_fields = [age]
  }
}
```

结果表 `fake1` 中的数据如下：

|   name   | card |
|----------|------|
| Joy Ding | 123  |
| May Ding | 123  |
| Kin Dom  | 123  |
| Joy Dom  | 123  |

### 多表

`env`、`source` 和 `sink` 部分与 [多表配置](#多表配置) 中的相同，只有 `transform` 部分不同。假设数据源输出 `test.abc`、`test.abcd`、`test.xyz` 和 `test.www` 四张表，每张表都有字段 `id`、`name`、`age`、`card`。

**1. 仅使用转换级别规则。** 由于 `table_match_regex` 默认为 `.*`，该规则应用于所有表。四张表的输出都是 `name, card`。

```hocon
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    include_fields = [name, card]
  }
}
```

**2. 转换级别规则加单表规则。** 完整的作业见 [多表配置](#多表配置)。

```hocon
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_match_regex = "test.a.*"
    include_fields = [name, card]
    table_transform = [
      {
        table_path = "test.xyz"
        exclude_fields = [age]
      }
    ]
  }
}
```

## 更新日志

### 新版本

- 添加过滤器转换连接器
