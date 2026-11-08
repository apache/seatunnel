# 字段重命名

> FieldRename 转换插件

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 描述

FieldRename 转换用于在不改变表名和行数据的前提下重命名表中的字段。它可以将字段名转换为大写或小写、添加前缀或后缀、按正则或精确匹配替换字段名，也可以显式重命名指定字段。主键和约束键的列会随字段一起重命名。FieldRename 支持多表配置，参见 [多表配置](#多表配置)。

## 主要特性

- 将所有字段名转换为 `UPPER` 或 `LOWER` 大小写
- 为所有字段名添加前缀和/或后缀
- 替换正则表达式的所有匹配，或按字段名精确匹配替换
- 显式重命名指定字段；`specific` 规则优先于其他所有重命名选项
- 主键和约束键的列会随字段一起重命名
- 一个转换即可处理多张表，参见 [多表转换](transform-multi-table.md)

## 选项

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| convert_case | Enum | 否 | - | 字段名大小写转换。支持 `UPPER`、`LOWER`。 |
| prefix | String | 否 | - | 添加到每个字段名前的前缀。 |
| suffix | String | 否 | - | 添加到每个字段名后的后缀。 |
| replacements_with_regex | Array&lt;Map&gt; | 否 | - | 替换规则。每个条目包含 `replace_from`、`replace_to` 以及可选的 `is_regex`（默认 `true`）。 |
| specific | Array&lt;Map&gt; | 否 | - | 显式重命名规则。每个条目包含 `field_name` 和 `target_name`。匹配后优先于其他所有重命名选项。 |
| table_match_regex | String | 否 | `.*` | 转换级别规则所适用的表路径的正则表达式。 |
| table_transform | Array&lt;Map&gt; | 否 | - | 针对单表的规则，参见 [多表转换](transform-multi-table.md)。 |
| rule_match_mode | String | 否 | `FIRST_MATCH` | 当多个 `table_transform` 条目的 `table_path` 相同时，决定它们的匹配方式。 |
| common options | - | 否 | - | 转换插件的通用参数，参见 [Transform Plugin](common-options/common-options.md)。 |

### convert_case [Enum]

将每个字段名转换为大写（`UPPER`）或小写（`LOWER`）。未设置时，字段名大小写保持不变。

### prefix [String]

为每个输出字段名添加前缀。前缀在应用 `convert_case` 和 `replacements_with_regex` 之后添加。值首尾的空白会被忽略。

### suffix [String]

为每个输出字段名添加后缀。后缀在应用 `convert_case` 和 `replacements_with_regex` 之后添加。值首尾的空白会被忽略。

### replacements_with_regex [Array&lt;Map&gt;]

替换规则列表。每个条目支持以下字段：

| 字段 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| replace_from | String | 是 | - | Java 正则表达式；当 `is_regex` 为 `false` 时，填写完整的字段名。 |
| replace_to | String | 是 | - | 替换文本。字段名中每个 `replace_from` 的匹配都会被替换为该文本。 |
| is_regex | Boolean | 否 | `true` | 为 `true` 时，`replace_from` 按正则表达式处理；为 `false` 时，`replace_from` 必须与完整字段名匹配。 |

字段名中每个 `replace_from` 的匹配都会被替换为 `replace_to`。匹配针对原始字段名进行，`convert_case` 不会影响哪些规则匹配。如果多个规则匹配同一个字段名，使用列表中最后一个匹配的规则。`replace_to` 首尾的空白会被忽略。

:::caution

每个条目都必须设置 `replace_from` 和 `replace_to`。`replace_from` 为空的条目会被忽略；如果某个条目匹配到字段但缺少 `replace_to`，作业会在初始化阶段失败。

:::

### specific [Array&lt;Map&gt;]

显式重命名规则列表。每个条目包含 `field_name` 和 `target_name`：

| 字段 | 类型 | 是否必须 | 描述 |
|------|------|----------|------|
| field_name | String | 是 | 要匹配的输入字段名。比较区分大小写。 |
| target_name | String | 是 | 输出字段名。 |

当字段匹配某条规则时，会被重命名为 `target_name`，并且不会对该字段应用 `convert_case`、`replacements_with_regex`、`prefix` 和 `suffix`。如果多个规则匹配同一个字段名，使用第一个规则。

### table_match_regex [String]

与上游表路径进行匹配的正则表达式。转换级别规则（位于 `table_transform` 之外的选项）只会应用于匹配的表。默认值 `.*` 匹配所有表。既不匹配该正则表达式、也没有 `table_transform` 条目的表，将保持不变地传递下去。

注意，这里匹配的是真实的上游表名，而不是 `plugin_output`。

### table_transform [Array&lt;Map&gt;]

针对单表的规则列表。每个条目必须包含 `table_path`（格式为 `databaseName[.schemaName].tableName`，精确匹配），以及要应用到该表的 FieldRename 选项。

当某张表在 `table_transform` 中有对应条目时，转换级别规则不会应用于该表。参见 [多表配置](#多表配置)。

### rule_match_mode [String]

控制当多个 `table_transform` 条目的 `table_path` 相同时的处理方式。支持的值为 `FIRST_MATCH`（只应用第一个匹配的条目）和 `ALL_MATCH`（按顺序应用所有匹配的条目，每个条目接收前一个条目的输出）。

如果未设置 `rule_match_mode`，重复的 `table_path` 条目会被拒绝。当多个条目使用相同的 `table_path` 时，请显式设置为 `FIRST_MATCH` 或 `ALL_MATCH`。

### common options

转换插件的通用参数，请参考 [Transform Plugin](common-options/common-options.md) 了解详情。

## 多表配置

当上游数据源输出多张表时（例如 JDBC 或 MySQL-CDC），一个 FieldRename 转换就可以处理所有这些表。

- `table_transform` 中的条目优先于转换级别规则。有对应条目的表不会再受转换级别规则影响。
- 每个条目必须包含 `table_path` 以及该表的 FieldRename 选项。
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
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    convert_case = "UPPER"
    prefix = "F_"

    table_transform = [
      {
        table_path = "test.xyz"
        convert_case = "LOWER"
        suffix = "_S"
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

| 表 | 应用的规则 | 输出字段 |
|----|------------|----------|
| test.abc | 转换级别规则（匹配 `test.a.*`） | F_ID, F_NAME, F_AGE |
| test.abcd | 转换级别规则（匹配 `test.a.*`） | F_ID, F_NAME, F_AGE |
| test.xyz | `table_transform` 条目 | id_s, name_s, age_s |
| test.www | 无 | id, name, age |

## 示例

### 单表

源端读取的数据是这样的一张表：

| InvoiceNum | VendorID |
|------------|----------|
| 1001 | Vendor A |
| 1002 | Vendor B |

将驼峰式字段名转换为下划线式：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      columns = [
        { name = "InvoiceNum", type = "bigint" },
        { name = "VendorID", type = "string" }
      ]
    }
    rows = [
      { kind = INSERT, fields = [1001, "Vendor A"] },
      { kind = INSERT, fields = [1002, "Vendor B"] }
    ]
  }
}

transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"

    convert_case = "LOWER"
    replacements_with_regex = [
      {
        replace_from = "(?<=[a-z0-9])(?=[A-Z])"
        replace_to = "_"
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

结果表 `fake1` 中的数据如下：

| invoice_num | vendor_id |
|-------------|-----------|
| 1001 | Vendor A |
| 1002 | Vendor B |

该正则匹配小写字母或数字与大写字母之间的位置。下划线会插入到该位置，因此 `InvoiceNum` 变为 `invoice_num`，`VendorID` 变为 `vendor_id`。

### 添加前缀和后缀

```hocon
transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"
    prefix = "f_"
    suffix = "_s"
  }
}
```

字段名会变为 `f_InvoiceNum_s` 和 `f_VendorID_s`。

### 重命名指定字段

```hocon
transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"
    specific = [
      { field_name = "InvoiceNum", target_name = "invoice_number" }
    ]
  }
}
```

`InvoiceNum` 变为 `invoice_number`，`VendorID` 保持不变。由于 `specific` 优先，匹配字段的 `convert_case`、`replacements_with_regex`、`prefix` 和 `suffix` 都会被忽略。

### 按精确匹配替换字段名

```hocon
transform {
  FieldRename {
    plugin_input = "fake"
    plugin_output = "fake1"
    replacements_with_regex = [
      {
        replace_from = "VendorID"
        replace_to = "vendor_id"
        is_regex = false
      }
    ]
  }
}
```

`VendorID` 变为 `vendor_id`，`InvoiceNum` 保持不变。当 `is_regex = false` 时，只有完整字段名等于 `replace_from` 的字段才会被替换。

### 多表

参见 [多表配置](#多表配置)。

## 更新日志

### 新版本

- 添加字段重命名转换连接器
