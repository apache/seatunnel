# Assert

> Assert sink connector

## Description

A sink plugin which can assert illegal data by user defined rules

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

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

Rule definition of user's available data.  Each rule represents one field validation or row num validation.

### field_rules [ConfigList]

field rules for field validation

### field_name [string]

field name（string）

### field_type [string | ConfigMap]

Field type declarations should adhere to this [guide](../../concept/schema-feature.md#how-to-declare-type-supported).

### field_value [ConfigList]

A list value rule define the data value validation

### rule_type [string]

The following rules are supported for now
- NOT_NULL `value can't be null`
- NULL `value can be null`
- MIN `define the minimum value of data`
- MAX `define the maximum value of data`
- MIN_LENGTH `define the minimum string length of a string data`
- MAX_LENGTH `define the maximum string length of a string data`
- MIN_ROW `define the minimun number of rows`
- MAX_ROW `define the maximum number of rows`

### rule_value [numeric]

The value related to rule type. When the `rule_type` is `MIN`, `MAX`, `MIN_LENGTH`, `MAX_LENGTH`, `MIN_ROW` or `MAX_ROW`, users need to assign a value to the `rule_value`.

### equals_to [boolean | numeric | string | ConfigList | ConfigMap]

`equals_to` is used to compare whether the field value is equal to the configured expected value. You can assign values of all types to `equals_to`. These types are detailed [here](../../concept/schema-feature.md#what-type-supported-at-now). For instance, if one field is a row with three fields, and the declaration of row type is `{a = array<string>, b = map<string, decimal(30, 2)>, c={c_0 = int, b = string}}`, users can assign the value `[["a", "b"], { k0 = 9999.99, k1 = 111.11 }, [123, "abcd"]]` to `equals_to`.

> The way of defining field values is consistent with [FakeSource](../source/FakeSource.md#customize-the-data-content-simple).
>
> `equals_to` cannot be applied to `null` type fields. However, users can use the rule type `NULL` for verification, such as `{rule_type = NULL}`.

### catalog_table_rule [ConfigMap]

Used to assert the catalog table is same with the user defined table.

### table-names [ConfigList]

Used to assert the table should be in the data.

### tables_configs [ConfigList]

Used to assert the multiple tables should be in the data.

### table_path [String]

The path of the table.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details

## Example

### Simple
the whole config obey with `hocon` style

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

### Complex

Here is a more complex example about `equals_to`. The example involves FakeSource. You may want to learn it, please read this [document](../source/FakeSource.md).

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

### Assert Multiple Tables 

check multiple tables

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

