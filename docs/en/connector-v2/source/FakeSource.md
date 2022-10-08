# FakeSource

> FakeSource connector

## Description

The FakeSource is a virtual data source, which randomly generates the number of rows according to the data structure of the user-defined schema,
just for some test cases such as type conversion or connector new feature testing

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| schema         | config | yes      | -             |
| row.num        | int    | no       | 5             |
| map.size       | int    | no       | 5             |
| array.size     | int    | no       | 5             |
| bytes.length   | int    | no       | 5             |
| string.length  | int    | no       | 5             |
| common-options |        | no       | -             |

### schema [config]

#### fields [Config]

The schema of fake data that you want to generate

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Examples

```hocon
  schema = {
    fields {
      c_map = "map<string, array<int>>"
      c_array = "array<int>"
      c_string = string
      c_boolean = boolean
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
      c_decimal = "decimal(30, 8)"
      c_null = "null"
      c_bytes = bytes
      c_date = date
      c_timestamp = timestamp
      c_row = {
        c_map = "map<string, map<string, string>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
```

### row.num

Total num of data that connector generated

### map.size

The size of `map` type that connector generated

### array.size

The size of `array` type that connector generated

### bytes.length

The length of `bytes` type that connector generated

### string.length

The length of `string` type that connector generated

## Example

```hocon
FakeSource {
  row.num = 10
  map.size = 10
  array.size = 10
  bytes.length = 10
  string.length = 10
  schema = {
    fields {
      c_map = "map<string, array<int>>"
      c_array = "array<int>"
      c_string = string
      c_boolean = boolean
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
      c_decimal = "decimal(30, 8)"
      c_null = "null"
      c_bytes = bytes
      c_date = date
      c_timestamp = timestamp
      c_row = {
        c_map = "map<string, map<string, string>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}
```
