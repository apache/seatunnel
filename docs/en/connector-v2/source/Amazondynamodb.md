# Amazondynamodb

> Amazondynamodb source connector

## Description

Read data from Amazondynamodb.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| url            | string | yes      | -             |
| region         | string | yes      | -             |
| accessKeyId    | string | yes      | -             |
| secretAccessKey| string | yes      | -             |
| table    	     | string | yes      | -             |
| schema         | object | yes      | -             |
| common-options |        | yes      | -             |

### url [string]

url to read to Amazondynamodb.

### region [string]

The region of Amazondynamodb.

### accessKeyId [string]

The access id of Amazondynamodb.

### secretAccessKey [string]

The access secret of Amazondynamodb.

### table [string]

The table of Amazondynamodb.

### schema [object]

#### fields [Config]

Amazon Dynamodb is a NOSQL database service of support keys-value storage and document data structure,there is no way to get the data type.Therefore, we must configure schma.

such as:

```
schema {
  fields {
    id = int
    key_aa = string
    key_bb = string
  }
}
```

### common options 

Source Plugin common parameters, refer to [Source Plugin](common-options.md) for details

## Example

```bash
  Amazondynamodb {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    accessKeyId = "dummy-key"
    secretAccessKey = "dummy-secret"
    table = "TableName"
    schema = {
      fields {
        artist = string
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
      }
    }
  }
```

## Changelog

### next version

- Add Amazondynamodb Source Connector