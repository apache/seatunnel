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
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Sink connector for Aerospike database.

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
| LIST                | LIST                | Support generic list types                                                   |
| DATE                | LONG                | Converted to epoch milliseconds                                              |
| TIMESTAMP           | LONG                | Converted to epoch milliseconds                                              |

Note:
- When using ARRAY type, SeaTunnel's array elements must be byte type
- LIST type supports any element types that can be serialized
- DATE/TIMESTAMP conversion uses system default time zone

## Options

| Name           | Type   | Required | Default | Description                                                                 |
|----------------|--------|----------|---------|-----------------------------------------------------------------------------|
| host           | string | Yes      | -       | Aerospike server hostname or IP address                                     |
| port           | int    | No       | 3000    | Aerospike server port                                                       |
| namespace      | string | Yes      | -       | Namespace in Aerospike                                                      |
| set            | string | Yes      | -       | Set name in Aerospike                                                       |
| username       | string | No       | -       | Username for authentication                                                |
| password       | string | No       | -       | Password for authentication                                                |
| key            | string | Yes      | -       | Field name to use as Aerospike primary key                                 |
| bin_name       | string | No       | -       | Bin name for storing data. Required when `data_format` is `map` or `string` |
| data_format    | string | No       | string  | Data storage format: map/string/kv                                         |
| write_timeout  | int    | No       | 200     | Write operation timeout in milliseconds                                    |
| schema.field   | map    | No       | {}      | Field type mappings (e.g. {"name":"STRING","age":"INTEGER"})               |

### data_format Options

- **map**: Store all non-key fields as a map in `bin_name`
- **string**: Store all non-key fields as a JSON string in `bin_name`
- **kv**: Store each non-key field as a separate bin. `bin_name` is not used

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
