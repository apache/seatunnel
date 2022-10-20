# Assert

> Assert sink connector

## Description

A flink sink plugin which can assert illegal data by user defined rules

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

## Options

| name                                        | type        | required | default value |
| ------------------------------------------- | ----------  | -------- | ------------- |
|rules                                        | ConfigMap   | yes      | -             |
|rules.field_rules                            | string      | yes      | -             |
|rules.field_rules.field_name                 | string      | yes      | -             |
|rules.field_rules.field_type                 | string      | no       | -             |
|rules.field_rules.field_value                | ConfigList  | no       | -             |
|rules.field_rules.field_value.rule_type      | string      | no       | -             |
|rules.field_rules.field_value.rule_value     | double      | no       | -             |
|rules.row_rules                              | string      | yes      | -             |
|rules.row_rules.rule_type                    | string      | no       | -             |
|rules.row_rules.rule_value                   | string      | no       | -             |
| common-options                              |             | no       | -             |

### rules [ConfigMap]

Rule definition of user's available data.  Each rule represents one field validation or row num validation.

### field_rules [ConfigList]

field rules for field validation

### field_name [string]

field name（string）

### field_type [string]

field type (string),  e.g. `string,boolean,byte,short,int,long,float,double,char,void,BigInteger,BigDecimal,Instant`

### field_value [ConfigList]

A list value rule define the data value validation

### rule_type [string]

The following rules are supported for now
- NOT_NULL `value can't be null`
- MIN `define the minimum value of data`
- MAX `define the maximum value of data`
- MIN_LENGTH `define the minimum string length of a string data`
- MAX_LENGTH `define the maximum string length of a string data`
- MIN_ROW `define the minimun number of rows`
- MAX_ROW `define the maximum number of rows`

### rule_value [double]

the value related to rule type

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example
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
      }

  }

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Assert Sink Connector

### 2.3.0-beta 2022-10-20
- [Improve] 1.Support check the number of rows ([2844](https://github.com/apache/incubator-seatunnel/pull/2844)) ([3031](https://github.com/apache/incubator-seatunnel/pull/3031)):
    - check rows not empty
    - check minimum number of rows
    - check maximum number of rows
- [Improve] 2.Support direct define of data values(row) ([2844](https://github.com/apache/incubator-seatunnel/pull/2844)) ([3031](https://github.com/apache/incubator-seatunnel/pull/3031))
- [Improve] 3.Support setting parallelism as 1 ([2844](https://github.com/apache/incubator-seatunnel/pull/2844)) ([3031](https://github.com/apache/incubator-seatunnel/pull/3031))
