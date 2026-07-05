import ChangeLog from '../changelog/connector-aerospike.md';

# Aerospike

> Aerospike sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## License Compatibility Notice

This connector depends on Aerospike Client Library which is licensed under AGPL 3.0.
When using this connector, you need to comply with AGPL 3.0 license terms.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Sink connector for Aerospike database. The connector writes records to one Aerospike namespace and set. It uses the configured `key` field as the Aerospike record key.

The connector writes to one fixed target set. It does not route records to different Aerospike sets by table name.

## Supported DataSource Info

| Datasource | Supported Versions | Maven                                                                                  |
|------------|-----------------|----------------------------------------------------------------------------------------|
| Aerospike  | 4.4.17+               | [Download](https://mvnrepository.com/artifact/com.aerospike/aerospike-client) |

## Data Type Mapping

| SeaTunnel Data Type | Aerospike Data Type | Storage Format                                                                 |
|---------------------|---------------------|--------------------------------------------------------------------------------|
| STRING              | STRING              | Direct string storage                                                         |
| INT                 | INTEGER             | 32-bit integer                                                                |
| BIGINT              | LONG                | 64-bit integer                                                                |
| DOUBLE              | DOUBLE              | 64-bit floating point                                                         |
| BOOLEAN             | BOOLEAN             | Stored as true/false values                                                   |
| ARRAY               | BYTEARRAY           | Only support byte array type                                                  |
| DATE                | LONG                | Converted to epoch milliseconds                                              |
| TIMESTAMP           | LONG                | Converted to epoch milliseconds                                              |

Note:
- When using `ARRAY`, SeaTunnel's array elements must be byte type.
- `LIST` is not inferred automatically from a SeaTunnel field type. Use `schema.field` to explicitly map a field to `LIST` when the incoming value is iterable.
- `DATE` and `TIMESTAMP` conversion uses the system default time zone.

## Options

| Name           | Type   | Required | Default | Description                                                                 |
|----------------|--------|----------|---------|-----------------------------------------------------------------------------|
| host           | string | Yes      | -       | Aerospike server hostname or IP address.                                    |
| port           | int    | Yes      | 3000    | Aerospike server port.                                                      |
| namespace      | string | Yes      | -       | Aerospike namespace.                                                        |
| set            | string | Yes      | -       | Aerospike set name.                                                         |
| username       | string | No       | -       | Username for authentication. Leave unset when authentication is disabled.    |
| password       | string | No       | -       | Password for authentication. Leave unset when authentication is disabled.    |
| key            | string | Yes      | -       | SeaTunnel field used as the Aerospike record key. The field must exist in the input schema. |
| bin_name       | string | No       | -       | Aerospike bin used by `map` and `string` formats. Required for those formats. |
| data_format    | string | No       | string  | Storage format. Supported values are `map`, `string`, and `kv`.             |
| write_timeout  | int    | No       | 200     | Write operation timeout in milliseconds.                                    |
| schema.field   | map    | No       | {}      | Field-to-Aerospike type mapping. For example: `c_id = "INTEGER"`.          |

### data_format Options

- **map**: Store all non-key fields as a map in `bin_name`
- **string**: Store all non-key fields as a JSON string in `bin_name`
- **kv**: Store each non-key field as a separate bin. `bin_name` is not used

When `data_format` is `map` or `string`, configure `bin_name`; otherwise the writer does not know which Aerospike bin should receive the packed row data.
The `key` field must be present in the input schema because it is used as the Aerospike record key for every write.

### schema.field

`schema.field` is optional for `map` and `string` formats. If it is omitted, the connector writes all input fields and maps the SeaTunnel field types automatically. Configure it when you need to control the Aerospike type used for each field.

For `kv` format, configure `schema.field` for the fields you want to write as independent Aerospike bins. The writer iterates over `schema.field`, so fields not listed there are not written in `kv` mode.

Supported Aerospike type names include `STRING`, `INTEGER`, `LONG`, `DOUBLE`, `BOOLEAN`, `BYTEARRAY`, and `LIST`.

## Task Example

### Write FakeSource Data To Aerospike

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 9
    string.fake.mode = "template"
    string.template = ["tyrantlucifer", "hailin", "kris", "fanjia", "zongwen", "gaojun"]
    int.fake.mode = "template"
    int.template = [20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
    double.fake.mode = "template"
    double.template = [44.0, 45.0, 46.0, 47.0]
    timestamp.fake.mode = "template"
    timestamp.template = [
      "2022-01-01 00:00:00",
      "2022-01-01 00:00:01",
      "2022-01-01 00:00:02",
      "2022-01-01 00:00:03"
    ]
    schema = {
      fields {
        c_id = "int"
        c_name = "string"
        c_money = "double"
        c_birth = "timestamp"
      }
    }
  }
}

sink {
  Aerospike {
    host = "aerospike-host"
    port = 3000
    namespace = "test"
    set = "seatunnel"
    key = "c_id"
    bin_name = "data"
    data_format = "string"
    username = ""
    password = ""
    schema {
      field {
        c_id = "INTEGER"
        c_name = "STRING"
        c_money = "DOUBLE"
        c_birth = "LONG"
      }
    }
  }
}
```

## Changelog

<ChangeLog />
