# 数据校验

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark

## 描述

DataValidator 根据可配置的校验规则检查字段值。它是一个行级数据质量闸门：校验通过的行原样传递，且输入模式永远不会被修改。当一行违反任意规则时，转换按配置的错误处理策略处理该行：使作业失败（`FAIL`，默认）、丢弃该行（`SKIP`），或将该行转换为结构化的错误记录并路由到专用的错误表（`ROUTE_TO_TABLE`）。

每一行的所有已配置字段都会被校验（不存在快速失败）：只要至少一条规则失败，该行即为无效，且该行的全部失败信息都会收集到错误消息中。

## 属性

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| field_rules | Array&lt;Map&gt; | 条件必需 | - | 按字段名分组的校验规则。转换级规则和每个 `table_transform` 条目都必填。 |
| row_error_handle_way | Enum | 否 | `FAIL` | 对无效行的处理方式：`FAIL`、`SKIP` 或 `ROUTE_TO_TABLE`。 |
| row_error_handle_way.error_table | String | 条件必需 | - | `row_error_handle_way` 为 `ROUTE_TO_TABLE` 时接收无效行的目标表。 |
| table_match_regex | String | 否 | `.*` | 匹配转换级规则所应用的上游表路径。 |
| table_transform | Array&lt;Map&gt; | 否 | `[]` | 通过精确的 `table_path` 为各表指定完整规则。 |
| rule_match_mode | Enum | 否 | `FIRST_MATCH` | 选择第一条匹配的表级规则，或按顺序执行所有匹配的规则。重复路径的行为见下文。 |
| common options | - | 否 | - | `plugin_input`、`plugin_output` 等转换插件通用参数。 |

### field_rules [Array&lt;Map&gt;]

校验规则。没有非空的 `table_transform` 列表时，转换级必须配置 `field_rules`；`table_transform = []` 不能免除这个要求。

每个条目描述一个字段的规则，支持两种等价的写法：

- **嵌套**：每个字段一个条目，内含 `rules` 列表：

```hocon
field_rules = [
  {
    field_name = "age"
    rules = [
      { rule_type = "NOT_NULL" },
      { rule_type = "RANGE", min_value = "0", max_value = "150" }
    ]
  }
]
```

- **扁平**：每条规则一个条目，规则参数直接写在条目里。同一字段的多个扁平条目会合并为一条字段规则：

```hocon
field_rules = [
  { field_name = "age", rule_type = "NOT_NULL" },
  { field_name = "age", rule_type = "RANGE", min_value = "0", max_value = "150" }
]
```

每个条目必须包含非空白的 `field_name`，且带 `rule_type` 或非空 `rules` 列表；否则任务提交时会因校验错误而失败。输入模式中不存在的字段的规则会带警告跳过，未知的规则类型也会带警告忽略。

支持的规则类型：

| rule_type | 参数 | 描述 |
|-----------|------|------|
| `NOT_NULL` | custom_message | 值为 `null` 时失败。 |
| `RANGE` | min_value、max_value、min_inclusive、max_inclusive、custom_message | 值超出范围时失败。默认两端均为闭区间。`null` 值会通过此规则——请与 `NOT_NULL` 组合使用来拒绝空值。 |
| `LENGTH` | min_length、max_length、exact_length、custom_message | 校验字符串、数组和集合的长度。先检查 `exact_length`，再检查 `min_length` / `max_length`。`null` 值会通过此规则。 |
| `REGEX` | pattern（必需）、case_sensitive、custom_message | 用 Java 正则表达式匹配值。默认区分大小写。 |
| `UDF` | function_name（必需）、custom_message | 执行内置或自定义的校验函数。内置：`EMAIL`（基于 OWASP 建议的实用邮箱校验）。 |

规则参数说明：

- **min_value / max_value**：两侧均为数值时按 `BigDecimal` 比较，因此字段类型不必与边界类型一致；无法解析为数字的值按其字符串表示比较。
- **min_inclusive / max_inclusive**：默认 `true`（闭区间）。设为 `false` 则该端为开区间。
- **min_length / max_length / exact_length**：整数。字符串长度按 Java UTF-16 代码单元计数；数组长度为元素个数（二进制数组按字节数）；集合长度为其大小。
- **case_sensitive**：默认 `true`。
- **custom_message**：替换规则的默认错误消息。它同时出现在 `FAIL` 模式的异常消息和错误表的 `validation_errors` 列中。
- 规则参数值可以写成数字或数字字符串（例如 `min_length = 2` 与 `min_length = "2"` 等价）。

### row_error_handle_way [Enum]

至少违反一条规则的行的处理方式：

- `FAIL`（默认）：转换抛出校验错误，作业失败。异常消息会合并该行所有失败规则的错误消息。
- `SKIP`：丢弃无效行；有效行继续向下游传递。
- `ROUTE_TO_TABLE`：无效行被转换为错误记录，路由到 `row_error_handle_way.error_table` 配置的错误表。未配置错误表（或值为空白）时，无效行会带警告丢弃。

:::caution
`ROUTE_TO_TABLE` 要求 sink 支持多表：转换会同时产出校验表和错误表，sink 必须能把两张表都写出去。
:::

### row_error_handle_way.error_table [String]

接收被路由无效行的表。取值为：

- 不含点号（.）的纯表名——错误表与输入表放在同一个数据库/schema 中；
- 含点号的路径（例如 `error_db.error_data` 或 `db.error_schema.error_data`）——按原样作为表路径使用。

每条被路由的行都是一条错误记录，固定模式如下：

| 列 | 类型 | 描述 |
|----|------|------|
| source_table_id | String | 该行来源表的完整标识符（catalog.database.schema.table，按实际存在的部分组成）。 |
| source_table_path | String | 源表的表路径。 |
| original_data | String | 完整原始行的 JSON 对象。 |
| validation_errors | String | `{field_name, error_message}` 对象的 JSON 数组，每条失败规则一个。 |
| create_time | LocalDateTime | 错误记录的产生时间。 |

### table_match_regex [String]

针对完整上游表路径匹配的 Java 正则表达式，例如 `test.abc` 或 `database.schema.table`。默认值 `.*` 匹配所有表路径。这里匹配的是实际表路径，不是 `plugin_input` 或 `plugin_output` 中的数据集名称。

此选项为转换级规则选择表，不限制显式的 `table_transform` 条目。

### table_transform [Array&lt;Map&gt;]

表级规则列表，默认为空列表。每个条目必须包含非空白的 `table_path`（精确路径，不是正则表达式），格式为 `databaseName[.schemaName].tableName`，外加该表一条完整的单表 DataValidator 规则：`field_rules` 和可选的错误处理策略。

条目声明自己的 `field_rules`，不继承转换级的 `field_rules`。即使该表不匹配 `table_match_regex`，匹配的条目也优先。错误处理策略是例外：见下文"多表配置"。

### rule_match_mode [Enum]

控制多个条目具有相同精确 `table_path` 时的行为：

- 显式设置 `FIRST_MATCH`：按列表顺序使用第一条匹配的条目。
- 显式设置 `ALL_MATCH`：按列表顺序执行所有匹配的条目；同一表中靠前条目的输出成为下一个匹配条目的输入。

虽然选项的默认值为 `FIRST_MATCH`，但省略此选项时，重复的 `table_path` 条目会在转换初始化阶段被拒绝。如果确实需要重复路径，请显式设置此选项。路径不重复时，使用默认值即可。

### common options

使用 `plugin_input` 和 `plugin_output` 连接上游和下游插件。通用参数详见[转换插件](common-options/common-options.md)。

## 多表配置

对于每个输入表，按照以下顺序选择有效规则：

1. 根据 `rule_match_mode` 使用匹配的一个或多个 `table_transform` 条目。
2. 否则，配置了转换级规则（见 `field_rules`）且表路径匹配 `table_match_regex` 时，使用转换级规则。
3. 否则，原样传递该表的行。

仅配置 `table_transform` 时，未列出的表原样传递。`field_rules` 的存在标志着转换级规则已配置：没有它，其余转换级选项对字段规则不产生作用。

DataValidator 有一个行为不同的选项组：`row_error_handle_way` 和 `row_error_handle_way.error_table` 是错误处理**策略**，在转换级配置一次，同时也作用于 `table_transform` 条目选择的表，除非条目自行设置。这样一张错误表就能服务于所有被校验的表。

下面的示例涵盖全部三种配置方式。共享的匹配规则详见[多表转换](transform-multi-table.md)。

## 示例

以下完整批处理配置使用 FakeSource 和 Console。在 SeaTunnel 发行包中安装这两个连接器后，可以将配置保存为文件，并通过 `bin/seatunnel.sh --config <file> -e local` 运行。行数由 FakeSource 的 `rows` 选项固定，因此预期输出是确定的。

### 仅使用转换级规则，配合 ROUTE_TO_TABLE

输入共六行，其中三行违反规则（`name` 为 null、age 为 200、邮箱格式错误）。配置 `ROUTE_TO_TABLE` 后，有效行继续流向校验输出，每条无效行则被转换为错误记录并路由到 `error_data`：

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    schema {
      fields {
        id = "int"
        name = "string"
        age = "int"
        email = "string"
      }
    }
    rows = [
      { fields = [1, "John Doe", 25, "john@example.com"], kind = INSERT }
      { fields = [2, "Jane Smith", 30, "jane@example.com"], kind = INSERT }
      { fields = [3, "Charlie Wilson", 32, "charlie@example.com"], kind = INSERT }
      { fields = [4, null, 30, "invalid@example.com"], kind = INSERT }
      { fields = [5, "Bob Johnson", 200, "bob@example.com"], kind = INSERT }
      { fields = [6, "Alice Brown", 28, "invalid-email"], kind = INSERT }
    ]
  }
}

transform {
  DataValidator {
    plugin_input = "fake"
    plugin_output = "validated"
    row_error_handle_way = "ROUTE_TO_TABLE"
    row_error_handle_way.error_table = "error_data"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
        custom_message = "name must not be null"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = "0"
        max_value = "150"
      },
      {
        field_name = "email"
        rule_type = "REGEX"
        pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
        custom_message = "invalid email format"
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "validated"
  }
}
```

`validated` 输出保留第 1–3 行不变。`error_data` 表接收三条错误记录；例如第 5 行的记录为：

| source_table_id | source_table_path | original_data | validation_errors | create_time |
|-----------------|-------------------|---------------|-------------------|-------------|
| ... | fake | {"id":5,"name":"Bob Johnson","age":200,"email":"bob@example.com"} | [{"field_name":"age","error_message":"Value 200 exceeds maximum 150"}] | ... |

第 6 行的记录在 `validation_errors` 中为 `[{"field_name":"email","error_message":"invalid email format"}]`，因为规则的 `custom_message` 替换了默认消息。

:::tip
配置 `row_error_handle_way = "FAIL"`（默认）时，同一配置在遇到第一条无效行时就会使作业失败；配置 `SKIP` 时三条无效行会被静默丢弃，只有第 1–3 行到达 sink。
:::

### 转换级规则与表级规则组合

每个输入表各含两行有效数据。`test.abc` 由转换级 `field_rules` 校验，`test.xyz` 使用自己的 `table_transform` 条目。错误处理策略（`FAIL`）在转换级设置一次，对两张表都生效。

| 表 | 有效字段规则 | 输出行数 |
|----|--------------|----------|
| `test.abc` | 转换级：`name` NOT_NULL，`age` RANGE 0–150。 | 2 |
| `test.xyz` | 表级条目：`name` NOT_NULL，`email` REGEX。 | 2 |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_input = "fake"
    tables_configs = [
      {
        schema {
          table = "test.abc"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "John Doe", 25, "john@example.com"], kind = INSERT }
          { fields = [2, "Jane Smith", 30, "jane@example.com"], kind = INSERT }
        ]
      },
      {
        schema {
          table = "test.xyz"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "Bob Johnson", 35, "bob@example.com"], kind = INSERT }
          { fields = [2, "Alice Brown", 28, "alice@example.com"], kind = INSERT }
        ]
      }
    ]
  }
}

transform {
  DataValidator {
    plugin_input = "fake"
    plugin_output = "validated"
    row_error_handle_way = "FAIL"
    // 应用于所有未被 table_transform 条目覆盖的表。
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = "0"
        max_value = "150"
      }
    ]

    table_transform = [
      {
        table_path = "test.xyz"
        field_rules = [
          {
            field_name = "name"
            rule_type = "NOT_NULL"
          },
          {
            field_name = "email"
            rule_type = "REGEX"
            pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
          }
        ]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "validated"
  }
}
```

### 仅使用表级规则

没有转换级 `field_rules`：每张表都由自己的 `table_transform` 条目校验，其他任何表都将原样传递。错误处理策略仍在转换级配置，作用于所有被校验的表。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema {
          table = "test.abc"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "John Doe", 25, "john@example.com"], kind = INSERT }
          { fields = [2, "Jane Smith", 30, "jane@example.com"], kind = INSERT }
        ]
      },
      {
        schema {
          table = "test.xyz"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" },
            { name = "email", type = "string" }
          ]
        }
        rows = [
          { fields = [1, "Bob Johnson", 35, "bob@example.com"], kind = INSERT }
          { fields = [2, "Alice Brown", 28, "alice@example.com"], kind = INSERT }
        ]
      }
    ]
  }
}

transform {
  DataValidator {
    plugin_input = "fake"
    plugin_output = "validated"
    row_error_handle_way = "FAIL"
    table_transform = [
      {
        table_path = "test.abc"
        field_rules = [
          {
            field_name = "name"
            rule_type = "NOT_NULL"
          },
          {
            field_name = "age"
            rule_type = "RANGE"
            min_value = "0"
            max_value = "150"
          }
        ]
      },
      {
        table_path = "test.xyz"
        field_rules = [
          {
            field_name = "name"
            rule_type = "NOT_NULL"
          },
          {
            field_name = "email"
            rule_type = "REGEX"
            pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
          }
        ]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "validated"
  }
}
```

此输入中两张表都有效，因此四行全部通过校验。若 `test.abc` 出现无效行，作业将失败，因为转换级 `row_error_handle_way = "FAIL"` 策略在没有转换级 `field_rules` 时依然生效；没有条目的表则不经校验直接通过。

## 开发自定义 UDF 规则

除内置的 `EMAIL` 函数外，你还可以添加自己的校验函数：

1. 实现 `DataValidatorUDF` 接口（`org.apache.seatunnel.transform.validator.udf.DataValidatorUDF`），返回唯一的 `functionName()` 和 `validate()` 逻辑：

```java
@AutoService(DataValidatorUDF.class)
public class PhoneValidator implements DataValidatorUDF {

    @Override
    public String functionName() {
        return "PHONE";
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        if (value == null) {
            return ValidationResult.success();
        }
        return value.toString().trim().matches("^\\+?[1-9]\\d{1,14}$")
                ? ValidationResult.success()
                : ValidationResult.failure("Invalid phone number format: " + value);
    }
}
```

2. 使用 `@AutoService(DataValidatorUDF.class)` 注解该类，使其被 Java 的 `ServiceLoader` 机制发现。
3. 把编译好的类打包为 JAR 并放入 SeaTunnel 类路径；之后该函数即可通过其 `function_name` 使用。
