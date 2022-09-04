# FakeSource

> FakeSource connector

## Description

The FakeSource is a virtual data source, which randomly generates the number of rows according to the data structure of the user-defined schema,
just for testing, such as type conversion and feature testing

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name              | type   | required | default value |
|-------------------|--------|----------|---------------|
| result_table_name | string | yes      | -             |
| schema            | config | yes      | -             |

### result_table_name [string]

The table name.

### type [string]
Table structure description ,you should assign schema option to tell connector how to parse data to the row you want.  
**Tips**: Most of Unstructured-Datasource contain this param, such as LocalFile,HdfsFile.  
**Example**:
```hocon
schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<tinyint>"
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
        c_time = time
        c_timestamp = timestamp
      }
    }
```

## Example
Simple source for FakeSource which contains enough datatype
```hocon
source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<tinyint>"
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
        c_time = time
        c_timestamp = timestamp
      }
    }
    result_table_name = "fake"
  }
}
```