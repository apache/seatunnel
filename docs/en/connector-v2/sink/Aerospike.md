# Aerospike

> Aerospike sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Description

Sink connector for Aerospike database.

## Supported DataSource Info

| Datasource | Supported Versions | Maven                                                                                  |
|------------|-----------------|----------------------------------------------------------------------------------------|
| Aerospike  | 4.4.17+               | [Download](https://mvnrepository.com/artifact/com.aerospike/aerospike-client) |

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

