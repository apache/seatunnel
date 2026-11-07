# 拆分

> 拆分转换插件

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 描述

Split 转换用于把一个输入字段拆分为多个输出字段。输入值会先通过 `toString()` 转成字符串，再按 `separator` 拆分，`separator` 是 Java 正则表达式。生成的列始终是 `STRING` 类型，并追加在输入列之后，原字段会保留。Split 支持多表配置，参见 [多表配置](#多表配置)。

## 主要特性

- 把一个字段拆分为固定数量的输出字段
- `separator` 是 Java 正则表达式，要按字面匹配正则元字符时必须转义
- 输入值为 null 时，所有输出字段均为 null
- 分段不足时用 null 补齐；分段过多时，剩余内容保留在最后一个输出字段中
- 一个转换即可处理多张表，参见 [多表转换](transform-multi-table.md)

## 选项

| 名称                | 类型             | 是否必须 | 默认值           | 描述                                                       |
|-------------------|----------------|------|---------------|----------------------------------------------------------|
| separator         | String         | 是    | -             | 用于拆分字段的 Java 正则表达式。                                      |
| split_field       | String         | 是    | -             | 需要拆分的输入字段，必须存在于输入表中。                                    |
| output_fields     | Array&lt;String&gt; | 是    | -             | 输出字段的名称，按顺序排列。列表不能为空。                                |
| table_match_regex | String         | 否    | `.*`          | 转换级别规则所适用的表路径的正则表达式。                                     |
| table_transform   | Array&lt;Map&gt;    | 否    | -             | 针对单表的规则，参见 [多表转换](transform-multi-table.md)。             |
| rule_match_mode   | String         | 否    | `FIRST_MATCH`（未显式设置时，重复的 `table_path` 条目会被拒绝） | 当多个 `table_transform` 条目的 `table_path` 相同时，决定它们的匹配方式。 |
| common options    | -              | 否    | -             | 转换插件的通用参数，参见 [Transform Plugin](common-options/common-options.md)。 |

:::info

每条规则中都必须包含 `separator`、`split_field` 和 `output_fields`。在多表配置中，可以省略转换级别规则，但每个 `table_transform` 条目仍必须包含这三个选项。没有匹配条目的表将保持不变地传递下去。

:::

### separator [String]

传给 `String#split` 的 Java 正则表达式，没有默认值。

由于该值是正则表达式，要按字面匹配元字符时必须转义：

- 单个空格：`" "`
- 逗号：`","`
- 字面竖线：`"\\|"`
- 字面点号：`"\\."`
- 一个或多个空格：`"\\s+"`

:::caution

不转义直接使用 `"|"` 是常见错误。作为正则表达式它表示空分支，拆分会在字符边界发生。当 `output_fields = [first_name, last_name]` 时，`Joy|Ding` 会拆成 `J` 和 `oy|Ding`，而不是 `Joy` 和 `Ding`。

:::

### split_field [String]

需要拆分的输入字段名称。拆分前会先通过 `toString()` 转换，因此其他类型的字段也可以使用。

该字段必须存在于输入表中，否则作业会因错误码 `TRANSFORM_COMMON-01` 失败：`The input field 'name' of 'Split' transform not found in upstream schema`。

### output_fields [Array&lt;String&gt;]

转换生成的输出字段名称，按顺序排列。列表不能为空。

每个输出列都是可为空的 `STRING` 列，长度为 200。这些列会追加在已有输入列之后，原输入字段不会被删除。

条目数量同时决定了输入值的拆分方式：

- 如果拆分结果的分段数少于 `output_fields`，剩余的输出字段为 `null`。
- 如果拆分结果的分段数多于 `output_fields`，最后一个输出字段会接收剩余的输入内容，包括剩余的分隔符。

例如，当 `separator = " "`、`output_fields = [first_name, last_name]` 时：

| 输入值             | first_name | last_name   |
|-----------------|------------|-------------|
| `Joy Ding`      | Joy        | Ding        |
| `Joy`           | Joy        | `null`      |
| `Joy Ding Junior` | Joy        | Ding Junior |
| `null`          | `null`     | `null`      |

### table_match_regex [String]

与上游表路径进行匹配的正则表达式。转换级别规则（位于 `table_transform` 之外的 `separator`、`split_field` 和 `output_fields`）只会应用于匹配的表。默认值 `.*` 匹配所有表。既不匹配该正则表达式、也没有 `table_transform` 条目的表，将保持不变地传递下去。

注意，这里匹配的是真实的上游表名，而不是 `plugin_output`。

### table_transform [Array&lt;Map&gt;]

针对单表的规则列表。每个条目必须包含 `table_path`（格式为 `databaseName[.schemaName].tableName`，精确匹配），以及该表的一条完整的 Split 规则，即 `separator`、`split_field` 和 `output_fields`。

当某张表在 `table_transform` 中有对应条目时，转换级别规则不会应用于该表。参见 [多表配置](#多表配置)。

### rule_match_mode [String]

控制当多个 `table_transform` 条目的 `table_path` 相同时的处理方式。支持的值为 `FIRST_MATCH`（只应用第一个匹配的条目）和 `ALL_MATCH`（按顺序应用所有匹配的条目，每个条目接收前一个条目的输出）。

如果未设置 `rule_match_mode`，重复的 `table_path` 条目会被拒绝。当多个条目使用相同的 `table_path` 时，请显式设置为 `FIRST_MATCH` 或 `ALL_MATCH`。

### common options

转换插件的通用参数，请参考 [Transform Plugin](common-options/common-options.md) 了解详情。

## 多表配置

当上游数据源输出多张表时（例如 JDBC 或 MySQL-CDC），一个 Split 转换就可以处理所有这些表。

- `table_transform` 中的条目优先于转换级别规则。有对应条目的表不会再受转换级别规则影响。
- 每个条目必须是一条完整的单表规则（`table_path` 加上 `separator`、`split_field` 和 `output_fields`）。
- 转换级别规则只应用于匹配 `table_match_regex` 的表。

下面的配置对 `test.a.*` 表使用转换级别规则，对 `test.xyz` 使用单表规则，并保持 `test.www` 不变：

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
          fields { id = int, name = string, age = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.abcd"
          fields { id = int, name = string, age = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.xyz"
          fields { id = int, name = string, age = int }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.www"
          fields { id = int, name = string, age = int }
        }
      }
    ]
  }
}

transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]

    table_transform = [
      {
        table_path = "test.xyz"
        separator = "-"
        split_field = "name"
        output_fields = [part1, part2]
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

各表的结果结构：

| 表          | 应用的规则                    | 输出字段                                 |
|------------|--------------------------|--------------------------------------|
| test.abc   | 转换级别规则（匹配 `test.a.*`）    | id, name, age, first_name, last_name |
| test.abcd  | 转换级别规则（匹配 `test.a.*`）    | id, name, age, first_name, last_name |
| test.xyz   | `table_transform` 条目     | id, name, age, part1, part2          |
| test.www   | 无                        | id, name, age                        |

## 示例

### 单表

源端数据读取的表格如下：

|   name   | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

我们想要将 `name` 字段拆分为 `first_name` 和 `last_name`，可以像这样添加 `Split` 转换：

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
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

那么结果表 `fake1` 中的数据将会像这样：

|   name   | age | card | first_name | last_name |
|----------|-----|------|------------|-----------|
| Joy Ding | 20  | 123  | Joy        | Ding      |
| May Ding | 20  | 123  | May        | Ding      |
| Kin Dom  | 20  | 123  | Kin        | Dom       |
| Joy Dom  | 20  | 123  | Joy        | Dom       |

### 多表

`env`、`source` 和 `sink` 块与 [多表配置](#多表配置) 中的相同，只有 `transform` 块会变化。假设源端输出 `test.abc`、`test.abcd`、`test.xyz` 和 `test.www`，每张表都包含 `id`、`name` 和 `age` 字段。

**1. 仅使用转换级别规则。** 由于 `table_match_regex` 默认为 `.*`，该规则会应用于所有表。

```hocon
transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]
  }
}
```

**2. 转换级别规则加单表规则。** 完整作业参见 [多表配置](#多表配置)。

```hocon
transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_match_regex = "test.a.*"
    separator = " "
    split_field = "name"
    output_fields = [first_name, last_name]
    table_transform = [
      {
        table_path = "test.xyz"
        separator = "-"
        split_field = "name"
        output_fields = [part1, part2]
      }
    ]
  }
}
```

**3. 仅使用单表规则。** 可以省略转换级别规则；没有匹配条目的表将保持不变地传递下去。

```hocon
transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_transform = [
      {
        table_path = "test.abc"
        separator = " "
        split_field = "name"
        output_fields = [first_name, last_name]
      },
      {
        table_path = "test.abcd"
        separator = " "
        split_field = "name"
        output_fields = [first_name, last_name]
      },
      {
        table_path = "test.xyz"
        separator = "-"
        split_field = "name"
        output_fields = [part1, part2]
      }
    ]
  }
}
```

`test.www` 没有匹配的条目，将保持不变地传递下去。

## 更新日志

### 新版本

- 添加拆分转换连接器
