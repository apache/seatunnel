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

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Description

Sink connector for Aerospike database.

## Supported DataSource Info

| Datasource | Supported Versions | Maven                                                                                  |
|------------|-----------------|----------------------------------------------------------------------------------------|
| Aerospike  | 4.4.17+               | [Download](https://mvnrepository.com/artifact/com.aerospike/aerospike-client) |

## Data Type Mapping

| SeaTunnel Data Type | Aerospike Data Type | Storage Format                                                                 |
|---------------------|---------------------|--------------------------------------------------------------------------------|
| STRING              | STRING              | Direct string storage                                                          |
| INT                 | INTEGER             | 32-bit integer                                                                 |
| BIGINT              | LONG                | 64-bit integer                                                                 |
| FLOAT               | FLOAT               | 32-bit floating point                                                          |
| DOUBLE              | DOUBLE              | 64-bit floating point                                                          |
| BOOLEAN             | BOOLEAN             | Stored as true/false values                                                    |
| MAP                 | MAP                 | JSON-style map structure                                                       |
| ARRAY               | LIST                | JSON-style array structure                                                     |
| DATE                | LONG                | Milliseconds since epoch (1970-01-01 00:00:00 UTC)                             |
| TIMESTAMP           | LONG                | Milliseconds since epoch (1970-01-01 00:00:00 UTC)                             |

Note:
- Complex types (MAP/LIST) require Aerospike server 4.4.0+ (matching connector dependency version)
- DATE/TIMESTAMP will be automatically converted to LONG type

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
| bin_name       | string | No       | -       | Bin name for storing data                                                  |
| data_format    | string | No       | string  | Data storage format: map/string/kv                                         |
| write_timeout  | int    | No       | 200     | Write operation timeout in milliseconds                                    |
| schema.field   | map    | No       | {}      | Field type mappings (e.g. {"name":"STRING","age":"INTEGER"})               |

### data_format Options
- **map**: Store data as JSON map
- **string**: Store data as JSON string
- **kv**: Store each field as separate bin

## Task Example

### Simple Example

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        address = "string"
      }
    }
  }
}

sink {
  Aerospike {
    host = "localhost"
    port = 3000
    namespace = "test_namespace"
    set = "user_data"
    key = "id"
    data_format = "map"
    write_timeout = 300
    schema.field = {
      id = "INTEGER"
      name = "STRING"
      age = "INTEGER"
      address = "STRING"
    }
  }
}
```
