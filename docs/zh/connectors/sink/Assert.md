import ChangeLog from '../changelog/connector-assert.md';

# Assert

> Assert 数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 描述

Assert 是一个用于校验任务输出结果的数据接收器。它可以按用户定义的规则检查行数、字段类型、字段值和 Catalog 表元数据。如果实际数据不符合规则，任务会失败。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 配置

| 参数名称                                                                                       | 类型                                            | 必填 | 默认值 |
|------------------------------------------------------------------------------------------------|-------------------------------------------------|----------|---------|
| rules                                                                                          | ConfigMap                                       | 是       | -       |
| rules.field_rules                                                                              | ConfigList                                      | 否       | -       |
| rules.field_rules.field_name                                                                   | string\|ConfigMap                               | 是       | -       |
| rules.field_rules.field_type                                                                   | string                                          | 否       | -       |
| rules.field_rules.field_value                                                                  | ConfigList                                      | 否       | -       |
| rules.field_rules.field_value.rule_type                                                        | string                                          | 否       | -       |
| rules.field_rules.field_value.rule_value                                                       | numeric                                         | 否       | -       |
| rules.field_rules.field_value.equals_to                                                        | boolean\|numeric\|string\|ConfigList\|ConfigMap | 否       | -       |
| rules.row_rules                                                                                | ConfigList                                      | 否       | -       |
| rules.row_rules.rule_type                                                                      | string                                          | 否       | -       |
| rules.row_rules.rule_value                                                                     | string                                          | 否       | -       |
| rules.catalog_table_rule                                                                       | ConfigMap                                       | 否       | -       |
| rules.catalog_table_rule.primary_key_rule                                                      | ConfigMap                                       | 否       | -       |
| rules.catalog_table_rule.primary_key_rule.primary_key_name                                     | string                                          | 否       | -       |
| rules.catalog_table_rule.primary_key_rule.primary_key_columns                                  | ConfigList                                      | 否       | -       |
| rules.catalog_table_rule.constraint_key_rule                                                   | ConfigList                                      | 否       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_name                               | string                                          | 否       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_type                               | string                                          | 否       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_columns                            | ConfigList                                      | 否       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_columns.constraint_key_column_name | string                                          | 否       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_columns.constraint_key_sort_type   | string                                          | 否       | -       |
| rules.catalog_table_rule.column_rule                                                           | ConfigList                                      | 否       | -       |
| rules.catalog_table_rule.column_rule.name                                                      | string                                          | 否       | -       |
| rules.catalog_table_rule.column_rule.type                                                      | string                                          | 否       | -       |
| rules.catalog_table_rule.column_rule.column_length                                             | int                                             | 否       | -       |
| rules.catalog_table_rule.column_rule.nullable                                                  | boolean                                         | 否       | -       |
| rules.catalog_table_rule.column_rule.default_value                                             | string                                          | 否       | -       |
| rules.catalog_table_rule.column_rule.comment                                                   | comment                                         | 否       | -       |
| rules.table-names                                                                              | ConfigList                                      | 否       | -       |
| rules.tables_configs                                                                           | ConfigList                                      | 否       | -       |
| rules.tables_configs.table_path                                                                | String                                          | 否       | -       |
| multi_table_sink_replica                                                                       | int                                             | 否       | -       |
| common-options                                                                                 |                                                 | 否       | -       |

连接器只强制要求配置 `rules`。`rules` 里面的各类规则块是可选的，但至少应配置一条有实际校验意义的规则，否则 Assert sink 没有可校验的内容。

### rules [ConfigMap]

定义期望数据的校验规则。每条规则可以用于字段校验、行数校验、表名校验或 Catalog 表元数据校验。

### field_rules [ConfigList]

字段规则用于字段校验。需要检查字段类型、是否为空、取值范围、字符串长度或精确值时使用。

### field_name [string]

字段名

### field_type [string | ConfigMap]

字段类型。字段类型应符合此[指南](../../introduction/concepts/schema-feature.md#如何声明支持的类型)。

### field_value [ConfigList]

字段值规则定义数据值验证

### rule_type [string]

规则类型。目前支持以下规则
- NOT_NULL `值不能为空`
- NULL `值可以为空`
- MIN `定义数据的最小值`
- MAX `定义数据的最大值`
- MIN_LENGTH `定义字符串数据的最小长度`
- MAX_LENGTH `定义字符串数据的最大长度`
- MIN_ROW `定义最小行数`
- MAX_ROW `定义最大行数`

### rule_value [numeric]

与规则类型相关的值。当`rule_type`为`MIN`、`MAX`、`MIN_LENGTH`、`MAX_LENGTH`、`MIN_ROW`或`MAX_ROW`时，用户需要为`rule_value`分配一个值。

### equals_to [boolean | numeric | string | ConfigList | ConfigMap]

`equals_to`用于比较字段值是否等于配置的预期值。用户可以将所有类型的值分配给`equals_to`。这些类型在[这里](../../introduction/concepts/schema-feature.md#目前支持哪些类型)有详细说明。
例如，如果一个字段是一个包含三个字段的行，行类型的声明是`{a = array<string>, b = map<string, decimal(30, 2)>, c={c_0 = int, b = string}}`，用户可以将值`[["a", "b"], { k0 = 9999.99, k1 = 111.11 }, [123, "abcd"]]`分配给`equals_to`。

> 定义字段值的方式与[FakeSource](../source/FakeSource.md#自定义数据内容简单示例)一致。
> 
> `equals_to`不能应用于`null`类型字段。但是，用户可以使用规则类型`NULL`进行验证，例如`{rule_type = NULL}`。

### catalog_table_rule [ConfigMap]

用于断言实际 Catalog 表元数据是否与用户定义的表元数据一致。

### table-names [ConfigList]

用于断言输入数据中是否包含指定表名。

### tables_configs [ConfigList]

用于为多表任务中的不同表配置不同的校验规则。每一项都应包含 `table_path`。

### table_path [String]

表的路径。

### multi_table_sink_replica [int]

多表写入通用参数中的副本数。只有在每张表需要多个 sink 副本时才需要配置。

### common options

Sink 插件的通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md) 了解详情

### 规则匹配说明

- `row_rules` 用于检查 Assert sink 收到的行数。
- `field_rules` 用于检查每一行中的字段值。
- `tables_configs` 用于多表任务，`table_path` 必须和上游数据携带的表路径一致。
- `equals_to` 会比较实际字段值和配置的期望值。数组、Map、Row 这类复杂值需要使用和 source 数据一致的 HOCON 写法。

:::tip

Assert 是一个终端 sink —— 没有外部存储系统可以写入。它适合在不需要下游数据库的情况下校验中间结果。连接器不会按 `UPDATE` 或 `DELETE` 行类型执行 CDC 语义，每条收到的记录都会按配置的规则进行断言。行数、字段值或 catalog 元数据校验失败时，任务会以对应的错误信息直接失败。

:::

## 流式校验

Assert 同时支持 `BATCH` 与 `STREAMING` 两种作业模式。字段规则（如 `NOT_NULL`、`MIN_LENGTH`、`MAX_LENGTH` 等）会在每一条记录到达 Sink Writer 时进行检查；行数规则（`MIN_ROW` / `MAX_ROW`）只在 Sink Writer 关闭时（作业关闭、savepoint 或失败时）执行一次，对比的是该 Writer 实例自创建以来累计接收的总行数，既不会按 checkpoint 窗口重复校验，也不会在 checkpoint 之间重置。如果需要真正按 checkpoint 窗口的行数校验，这超出了文档更新范围，需要改动源代码。

## 示例

### 简单
下面的示例校验任务输出行数在 5 到 100 之间，并校验选中字段符合预期规则。

```hocon
Assert {
    rules =
      {
        row_rules = [
          {
            rule_type = MAX_ROW
            rule_value = 10
          },
          {
            rule_type = MIN_ROW
            rule_value = 5
          }
        ],
        field_rules = [{
          field_name = name
          field_type = string
          field_value = [
            {
              rule_type = NOT_NULL
            },
            {
              rule_type = MIN_LENGTH
              rule_value = 5
            },
            {
              rule_type = MAX_LENGTH
              rule_value = 10
            }
          ]
        }, {
          field_name = age
          field_type = int
          field_value = [
            {
              rule_type = NOT_NULL
              equals_to = 23
            },
            {
              rule_type = MIN
              rule_value = 32767
            },
            {
              rule_type = MAX
              rule_value = 2147483647
            }
          ]
        }
        ]
        catalog_table_rule {
            primary_key_rule = {
                primary_key_name = "primary key"
                primary_key_columns = ["id"]
            }
            constraint_key_rule = [
                        {
                        constraint_key_name = "unique_name"
                        constraint_key_type = UNIQUE_KEY
                        constraint_key_columns = [
                            {
                                constraint_key_column_name = "id"
                                constraint_key_sort_type = ASC
                            }
                        ]
                        }
            ]
            column_rule = [
               {
                name = "id"
                type = bigint
               },
              {
                name = "name"
                type = string
              },
              {
                name = "age"
                type = int
              }
            ]
        }
      }

  }
```

### 复杂

这里有一个更复杂的例子，涉及到`equals_to`。

```hocon
source {
  FakeSource {
    row.num = 1
    schema = {
      fields {
        c_null = "null"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_date = date
        c_timestamp = timestamp
        c_time = time
        c_bytes = bytes
        c_array = "array<int>"
        c_map = "map<time, string>"
        c_map_nest = "map<string, {c_int = int, c_string = string}>"
        c_row = {
          c_null = "null"
          c_string = string
          c_boolean = boolean
          c_tinyint = tinyint
          c_smallint = smallint
          c_int = int
          c_bigint = bigint
          c_float = float
          c_double = double
          c_decimal = "decimal(30, 8)"
          c_date = date
          c_timestamp = timestamp
          c_time = time
          c_bytes = bytes
          c_array = "array<int>"
          c_map = "map<string, string>"
        }
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [
          null, "AAA", false, 1, 1, 333, 323232, 3.1, 9.33333, 99999.99999999, "2012-12-21", "2012-12-21T12:34:56", "12:34:56",
          "bWlJWmo=",
          [0, 1, 2],
          "{ 12:01:26 = v0 }",
          { k1 = [123, "BBB-BB"]},
          [
            null, "AAA", false, 1, 1, 333, 323232, 3.1, 9.33333, 99999.99999999, "2012-12-21", "2012-12-21T12:34:56", "12:34:56",
            "bWlJWmo=",
            [0, 1, 2],
            { k0 = v0 }
          ]
        ]
      }
    ]
    plugin_output = "fake"
  }
}

sink{
  Assert {
    plugin_input = "fake"
    rules =
      {
        row_rules = [
          {
            rule_type = MAX_ROW
            rule_value = 1
          },
          {
            rule_type = MIN_ROW
            rule_value = 1
          }
        ],
        field_rules = [
            {
                field_name = c_null
                field_type = "null"
                field_value = [
                    {
                        rule_type = NULL
                    }
                ]
            },
            {
                field_name = c_string
                field_type = string
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = "AAA"
                    }
                ]
            },
            {
                field_name = c_boolean
                field_type = boolean
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = false
                    }
                ]
            },
            {
                field_name = c_tinyint
                field_type = tinyint
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = 1
                    }
                ]
            },
            {
                field_name = c_smallint
                field_type = smallint
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = 1
                    }
                ]
            },
            {
                field_name = c_int
                field_type = int
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = 333
                    }
                ]
            },
            {
                field_name = c_bigint
                field_type = bigint
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = 323232
                    }
                ]
            },
            {
                field_name = c_float
                field_type = float
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = 3.1
                    }
                ]
            },
            {
                field_name = c_double
                field_type = double
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = 9.33333
                    }
                ]
            },
            {
                field_name = c_decimal
                field_type = "decimal(30, 8)"
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = 99999.99999999
                    }
                ]
            },
            {
                field_name = c_date
                field_type = date
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = "2012-12-21"
                    }
                ]
            },
            {
                field_name = c_timestamp
                field_type = timestamp
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = "2012-12-21T12:34:56"
                    }
                ]
            },
            {
                field_name = c_time
                field_type = time
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = "12:34:56"
                    }
                ]
            },
            {
                field_name = c_bytes
                field_type = bytes
                field_value = [
                      {
                          rule_type = NOT_NULL
                          equals_to = "bWlJWmo="
                      }
                ]
            },
            {
                field_name = c_array
                field_type = "array<int>"
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = [0, 1, 2]
                    }
                ]
            },
            {
                field_name = c_map
                field_type = "map<time, string>"
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = "{ 12:01:26 = v0 }"
                    }
                ]
            },
            {
                field_name = c_map_nest
                field_type = "map<string, {c_int = int, c_string = string}>"
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = { k1 = [123, "BBB-BB"] }
                    }
                ]
            },
            {
                field_name = c_row
                field_type = {
                    c_null = "null"
                    c_string = string
                    c_boolean = boolean
                    c_tinyint = tinyint
                    c_smallint = smallint
                    c_int = int
                    c_bigint = bigint
                    c_float = float
                    c_double = double
                    c_decimal = "decimal(30, 8)"
                    c_date = date
                    c_timestamp = timestamp
                    c_time = time
                    c_bytes = bytes
                    c_array = "array<int>"
                    c_map = "map<string, string>"
                }
                field_value = [
                    {
                        rule_type = NOT_NULL
                        equals_to = [
                           null, "AAA", false, 1, 1, 333, 323232, 3.1, 9.33333, 99999.99999999, "2012-12-21", "2012-12-21T12:34:56", "12:34:56",
                           "bWlJWmo=",
                           [0, 1, 2],
                           { k0 = v0 }
                        ]
                    }
                ]
            }
        ]
    }
  }
}
```

### 验证多表

下面的示例在一个任务中校验两张表，每张表都有独立的行数规则和字段规则。

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}

source {
  FakeSource {
    tables_configs = [
      {
        row.num = 16
        schema {
          table = "test.table1"
          fields {
            c_int = int
            c_bigint = bigint
          }
        }
      },
      {
        row.num = 17
        schema {
          table = "test.table2"
          fields {
            c_string = string
            c_tinyint = tinyint
          }
        }
      }
    ]
  }
}

transform {
}

sink {
  Assert {
    rules =
      {
        tables_configs = [
          {
            table_path = "test.table1"
            row_rules = [
              {
                rule_type = MAX_ROW
                rule_value = 16
              },
              {
                rule_type = MIN_ROW
                rule_value = 16
              }
            ],
            field_rules = [{
              field_name = c_int
              field_type = int
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            }, {
              field_name = c_bigint
              field_type = bigint
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            }]
          },
          {
            table_path = "test.table2"
            row_rules = [
              {
                rule_type = MAX_ROW
                rule_value = 17
              },
              {
                rule_type = MIN_ROW
                rule_value = 17
              }
            ],
            field_rules = [{
              field_name = c_string
              field_type = string
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            }, {
              field_name = c_tinyint
              field_type = tinyint
              field_value = [
                {
                  rule_type = NOT_NULL
                }
              ]
            }]
          }
        ]

      }
  }
}

```

### 流式校验并按 Checkpoint 窗口断言行数

下面的示例演示一个流式作业：作业结束时累计行数满足 `MIN_ROW` / `MAX_ROW` 区间（`50 ≤ 总行数 ≤ 5000`）。该校验只在 Writer 关闭时执行一次，对比累计行数，并不是按 checkpoint 窗口重复执行。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  FakeSource {
    row.num = 1000
    schema = {
      fields {
        name = string
        age = int
      }
    }
    plugin_output = "stream_data"
  }
}

sink {
  Assert {
    plugin_input = "stream_data"
    rules =
      {
        row_rules = [
          {
            rule_type = MIN_ROW
            rule_value = 50
          },
          {
            rule_type = MAX_ROW
            rule_value = 5000
          }
        ],
        field_rules = [{
          field_name = age
          field_type = int
          field_value = [
            {
              rule_type = NOT_NULL
            },
            {
              rule_type = MIN
              rule_value = 0
            },
            {
              rule_type = MAX
              rule_value = 150
            }
          ]
        }]
      }
  }
}
```

## 变更日志

<ChangeLog />
