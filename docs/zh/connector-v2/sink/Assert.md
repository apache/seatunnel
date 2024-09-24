# Assert

> Assert 数据接收器

## 描述

Assert 数据接收器是一个用于断言数据是否符合用户定义规则的数据接收器。用户可以通过配置规则来断言数据是否符合预期，如果数据不符合规则，将会抛出异常。

## 核心特性

- [ ] [精准一次](../../concept/connector-v2-features.md)

## 配置

| Name                                                                                           | Type                                            | Required | Default |
|------------------------------------------------------------------------------------------------|-------------------------------------------------|----------|---------|
| rules                                                                                          | ConfigMap                                       | yes      | -       |
| rules.field_rules                                                                              | string                                          | yes      | -       |
| rules.field_rules.field_name                                                                   | string\|ConfigMap                               | yes      | -       |
| rules.field_rules.field_type                                                                   | string                                          | no       | -       |
| rules.field_rules.field_value                                                                  | ConfigList                                      | no       | -       |
| rules.field_rules.field_value.rule_type                                                        | string                                          | no       | -       |
| rules.field_rules.field_value.rule_value                                                       | numeric                                         | no       | -       |
| rules.field_rules.field_value.equals_to                                                        | boolean\|numeric\|string\|ConfigList\|ConfigMap | no       | -       |
| rules.row_rules                                                                                | string                                          | yes      | -       |
| rules.row_rules.rule_type                                                                      | string                                          | no       | -       |
| rules.row_rules.rule_value                                                                     | string                                          | no       | -       |
| rules.catalog_table_rule                                                                       | ConfigMap                                       | no       | -       |
| rules.catalog_table_rule.primary_key_rule                                                      | ConfigMap                                       | no       | -       |
| rules.catalog_table_rule.primary_key_rule.primary_key_name                                     | string                                          | no       | -       |
| rules.catalog_table_rule.primary_key_rule.primary_key_columns                                  | ConfigList                                      | no       | -       |
| rules.catalog_table_rule.constraint_key_rule                                                   | ConfigList                                      | no       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_name                               | string                                          | no       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_type                               | string                                          | no       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_columns                            | ConfigList                                      | no       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_columns.constraint_key_column_name | string                                          | no       | -       |
| rules.catalog_table_rule.constraint_key_rule.constraint_key_columns.constraint_key_sort_type   | string                                          | no       | -       |
| rules.catalog_table_rule.column_rule                                                           | ConfigList                                      | no       | -       |
| rules.catalog_table_rule.column_rule.name                                                      | string                                          | no       | -       |
| rules.catalog_table_rule.column_rule.type                                                      | string                                          | no       | -       |
| rules.catalog_table_rule.column_rule.column_length                                             | int                                             | no       | -       |
| rules.catalog_table_rule.column_rule.nullable                                                  | boolean                                         | no       | -       |
| rules.catalog_table_rule.column_rule.default_value                                             | string                                          | no       | -       |
| rules.catalog_table_rule.column_rule.comment                                                   | comment                                         | no       | -       |
| rules.table-names                                                                              | ConfigList                                      | no       | -       |
| rules.tables_configs                                                                           | ConfigList                                      | no       | -       |
| rules.tables_configs.table_path                                                                | String                                          | no       | -       |
| common-options                                                                                 |                                                 | no       | -       |

### rules [ConfigMap]

规则定义用户可用数据的规则。每个规则代表一个字段验证或行数量验证。

### field_rules [ConfigList]

字段规则用于字段验证

### field_name [string]

字段名

### field_type [string | ConfigMap]

字段类型。字段类型应符合此[指南](../../concept/schema-feature.md#how-to-declare-type-supported)。

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

`equals_to`用于比较字段值是否等于配置的预期值。用户可以将所有类型的值分配给`equals_to`。这些类型在[这里](../../concept/schema-feature.md#what-type-supported-at-now)有详细说明。
例如，如果一个字段是一个包含三个字段的行，行类型的声明是`{a = array<string>, b = map<string, decimal(30, 2)>, c={c_0 = int, b = string}}`，用户可以将值`[["a", "b"], { k0 = 9999.99, k1 = 111.11 }, [123, "abcd"]]`分配给`equals_to`。

> 定义字段值的方式与[FakeSource](../source/FakeSource.md#customize-the-data-content-simple)一致。
> 
> `equals_to`不能应用于`null`类型字段。但是，用户可以使用规则类型`NULL`进行验证，例如`{rule_type = NULL}`。

### catalog_table_rule [ConfigMap]

catalog_table_rule用于断言Catalog表是否与用户定义的表相同。

### table-names [ConfigList]

用于断言表是否在数据中。

### tables_configs [ConfigList]

用于断言多个表是否在数据中。

### table_path [String]

表的路径。

### common options

Sink 插件的通用参数，请参考 [Sink Common Options](../sink-common-options.md) 了解详情

## 示例

### 简单
整个Config遵循`hocon`风格

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
    result_table_name = "fake"
  }
}

sink{
  Assert {
    source_table_name = "fake"
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

验证多个表

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

