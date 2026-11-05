# 正则提取

> 正则提取转换插件

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 描述

RegexExtract 转换使用 Java 正则表达式从一个输入字段中提取数据，并将每个捕获组的值写入一个新的输出字段。它通常用于在数据接入过程中，把电子邮件地址、日志行、文件路径等半结构化字符串拆分为独立的列。当模式不匹配，或者输入值为 null 时，每个输出字段会改用其配置的默认值（未配置默认值时为 null）。RegexExtract 支持多表配置，参见 [多表配置](#多表配置)。

## 主要特性

- 从单个源字段中按捕获组逐个提取输出字段
- 模式是在值中搜索的（`Matcher#find`），不必匹配整个值
- `default_values` 为 null 值和不匹配的值提供兜底结果
- 生成的列始终是 `STRING` 类型，并追加在输入列之后，原字段会保留
- 一个转换即可处理多张表，参见 [多表转换](transform-multi-table.md)

## 选项

| 名称                | 类型             | 是否必须 | 默认值           | 描述                                                       |
|-------------------|----------------|------|---------------|----------------------------------------------------------|
| source_field      | String         | 是    | -             | 需要提取数据的输入字段，必须存在于输入表中。                                    |
| regex_pattern     | String         | 是    | -             | 带捕获组的正则表达式。捕获组的数量必须与 `output_fields` 的数量相等。                                |
| output_fields     | Array&lt;String&gt; | 是    | -             | 输出字段的名称，按捕获组顺序排列。列表不能为空。                                |
| default_values    | Array&lt;String&gt; | 否    | -             | 每个输出字段的默认值，在模式不匹配或源值为 null 时使用。设置时，其长度必须与 `output_fields` 的长度相等。 |
| table_match_regex | String         | 否    | `.*`          | 转换级别规则所适用的表路径的正则表达式。                                     |
| table_transform   | Array&lt;Map&gt;    | 否    | -             | 针对单表的规则，参见 [多表转换](transform-multi-table.md)。             |
| rule_match_mode   | String         | 否    | `FIRST_MATCH`（未显式设置时，重复的 `table_path` 条目会被拒绝） | 当多个 `table_transform` 条目的 `table_path` 相同时，决定它们的匹配方式。 |
| common options    | -              | 否    | -             | 转换插件的通用参数，参见 [Transform Plugin](common-options/common-options.md)。 |

:::info

每条规则中都必须包含 `source_field`、`regex_pattern` 和 `output_fields`。在多表配置中，可以省略转换级别规则，但每个 `table_transform` 条目仍必须包含这三个选项。没有匹配条目的表将保持不变地传递下去。

:::

### source_field [String]

需要提取数据的输入字段名称。匹配前会先通过 `toString()` 转换，因此其他类型的字段也可以使用。

该字段必须存在于输入表中，否则作业会因错误码 `TRANSFORM_COMMON-01` 失败：`The input field 'email' of 'RegexExtract' transform not found in upstream schema`。

### regex_pattern [String]

Java 正则表达式，使用 `java.util.regex.Pattern` 编译。它必须为 `output_fields` 的每个条目包含一个捕获组：如果捕获组的数量与输出字段的数量不同，作业会在创建转换时失败。

该模式通过 `Matcher#find` 应用，也就是说它会在值的任意位置搜索，不必匹配整个值，并使用第一个匹配结果。未参与匹配的捕获组会为其输出字段产生 `null`。

由于模式写在 HOCON 字符串中，反斜杠必须转义，例如 `regex_pattern = "([^@]+)@([^.]+)\\.(.+)"`。

### output_fields [Array<String>]

转换生成的输出字段名称，按捕获组顺序排列：第一个输出字段接收捕获组 1，第二个接收捕获组 2，依此类推。列表不能为空。

每个输出列都是可为空的 `STRING` 列，长度为 200。这些列会追加在已有输入列之后，原输入字段不会被删除。如果某个输出字段名已存在于输入表中，该列会被复用并覆盖，而不是追加新列。

### default_values [Array<String>]

每个输出字段的兜底值，顺序与 `output_fields` 相同。以下两种情况会使用默认值：

- 源值为 `null`；
- 源值与 `regex_pattern` 不匹配。

设置 `default_values` 时，其长度必须与 `output_fields` 的长度相等，否则配置会被拒绝。未设置时，上述两种情况下输出字段均为 `null`。

### table_match_regex [String]

与上游表路径匹配的正则表达式。转换级别规则（即 `table_transform` 之外的 `source_field`、`regex_pattern`、`output_fields` 和 `default_values`）仅适用于匹配的表。默认值 `.*` 匹配所有表。既不匹配该正则、也没有 `table_transform` 条目的表将保持不变地传递下去。

注意，这里指的是真实的上游表名，而不是 `plugin_output`。

### table_transform [Array<Map>]

针对单表的规则列表。每个条目必须包含 `table_path`，其格式为 `databaseName[.schemaName].tableName`（精确匹配），以及该表的一条完整 RegexExtract 规则，即 `source_field`、`regex_pattern` 和 `output_fields`，需要时还可以包含 `default_values`。

当某张表在 `table_transform` 中有条目时，转换级别规则不会应用于该表。参见 [多表配置](#多表配置)。

### rule_match_mode [String]

当多个 `table_transform` 条目具有相同的 `table_path` 时，决定如何处理。支持的值有 `FIRST_MATCH`（仅应用第一个匹配的条目）和 `ALL_MATCH`（按顺序应用所有匹配的条目，后一个条目接收前一个条目的输出）。

如果未设置 `rule_match_mode`，重复的 `table_path` 条目会被拒绝。当多个条目使用相同的 `table_path` 时，请显式将其设置为 `FIRST_MATCH` 或 `ALL_MATCH`。

### common options

转换插件的常见参数，请参考 [Transform Plugin](common-options/common-options.md) 了解详情。

## 多表配置

当上游源输出多张表时（例如 JDBC 或 MySQL-CDC），一个 RegexExtract 转换可以处理所有这些表。

- `table_transform` 中的条目优先于转换级别规则。有条目的表永远不会被转换级别规则处理。
- 每个条目都必须是一条完整的单表规则（`table_path` 加上 `source_field`、`regex_pattern` 和 `output_fields`）。
- 转换级别规则仅适用于与 `table_match_regex` 匹配的表。
- 当仅配置了 `table_transform` 时，没有条目的表保持不变。

下面的配置为 `test.a.*` 表使用转换级别规则，为 `test.xyz` 使用单表规则，并让 `test.www` 保持不变：

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
          fields { id = int, email = string }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.abcd"
          fields { id = int, email = string }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.xyz"
          fields { id = int, email = string }
        }
      },
      {
        row.num = 2
        schema = {
          table = "test.www"
          fields { id = int, email = string }
        }
      }
    ]
  }
}

transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = [username, domain, tld]

    table_transform = [
      {
        table_path = "test.xyz"
        source_field = "email"
        regex_pattern = "([^@]+)@(.+)"
        output_fields = [username, host]
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

| 表         | 应用的规则                              | 输出字段                           |
|------------|-------------------------------------------|----------------------------------|
| test.abc   | 转换级别规则（匹配 `test.a.*`） | id, email, username, domain, tld |
| test.abcd  | 转换级别规则（匹配 `test.a.*`） | id, email, username, domain, tld |
| test.xyz   | `table_transform` 条目                   | id, email, username, host        |
| test.www   | 无                                      | id, email                        |

## 示例

### 单表

源端数据读取的表格如下：

| id | email             | log_entry                                            |
|----|-------------------|------------------------------------------------------|
| 1  | user1@example.com | 2023-12-01 10:30:45 INFO User login successful       |
| 2  | admin@test.org    | 2023-12-01 11:15:22 ERROR Database connection failed |
| 3  | guest@domain.net  | 2023-12-01 12:00:00 WARN Memory usage high           |

我们想要从 `email` 字段中提取用户名、域名和顶级域名：

```hocon
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
        email = "string"
        log_entry = "string"
      }
    }
    rows = [
      {
          kind = INSERT,
          fields = [1, "user1@example.com", "2023-12-01 10:30:45 INFO User login successful"]
      },
      {
        kind = INSERT,
        fields = [2, "admin@test.org", "2023-12-01 11:15:22 ERROR Database connection failed"]
      },
      {
        kind = INSERT,
        fields = [3, "guest@domain.net", "2023-12-01 12:00:00 WARN Memory usage high"]
      }
    ]
  }
}

transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}

sink {
  Console {
    plugin_input = "regex_result"
  }
}
```

那么结果表 `regex_result` 中的数据将会更新为：

| id | email             | log_entry                                            | username | domain  | tld |
|----|-------------------|------------------------------------------------------|----------|---------|-----|
| 1  | user1@example.com | 2023-12-01 10:30:45 INFO User login successful       | user1    | example | com |
| 2  | admin@test.org    | 2023-12-01 11:15:22 ERROR Database connection failed | admin    | test    | org |
| 3  | guest@domain.net  | 2023-12-01 12:00:00 WARN Memory usage high           | guest    | domain  | net |

对于 `email` 与模式不匹配或为 `null` 的行，由于这里配置了 `default_values`，三个输出字段都会得到 `unknown`。如果没有配置 `default_values`，这类行的三个输出字段都将是 `null`。

### 多表

`env`、`source` 和 `sink` 部分与 [多表配置](#多表配置) 中相同，仅 `transform` 部分不同。假设源输出 `test.abc`、`test.abcd`、`test.xyz` 和 `test.www` 四张表，每张表都包含 `id` 和 `email` 字段。

**1. 仅转换级别规则。** 由于 `table_match_regex` 默认为 `.*`，该规则适用于所有表。

```hocon
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = [username, domain, tld]
  }
}
```

**2. 转换级别规则加单表规则。** 完整的作业配置参见 [多表配置](#多表配置)。

```hocon
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_match_regex = "test.a.*"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = [username, domain, tld]
    table_transform = [
      {
        table_path = "test.xyz"
        source_field = "email"
        regex_pattern = "([^@]+)@(.+)"
        output_fields = [username, host]
      }
    ]
  }
}
```

**3. 仅单表规则。** 可以省略转换级别规则；没有条目的表将保持不变地传递下去。

```hocon
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "fake1"
    table_transform = [
      {
        table_path = "test.abc"
        source_field = "email"
        regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
        output_fields = [username, domain, tld]
      },
      {
        table_path = "test.abcd"
        source_field = "email"
        regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
        output_fields = [username, domain, tld]
      },
      {
        table_path = "test.xyz"
        source_field = "email"
        regex_pattern = "([^@]+)@(.+)"
        output_fields = [username, host]
      }
    ]
  }
}
```

`test.www` 没有匹配的条目，将保持不变地传递下去。

## 更新日志

### 新版本

- 添加 RegexExtract Transform 插件
