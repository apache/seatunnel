import ChangeLog from '../changelog/connector-influxdb.md';

# InfluxDB

> InfluxDB sink connector

## Description

Write SeaTunnel rows to InfluxDB 1.x. The sink converts one row into one InfluxDB point, using
`measurement`, `key_time`, and `key_tags` to decide the target measurement, timestamp, tags, and
fields.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Options

| name                        | type   | required | default value         | description                                                                                   |
|-----------------------------|--------|----------|-----------------------|-----------------------------------------------------------------------------------------------|
| url                         | string | yes      | -                     | InfluxDB server URL, for example `http://influxdb-host:8086`.                                 |
| database                    | string | yes      | -                     | InfluxDB database name.                                                                       |
| measurement                 | string | no       | input table full name | InfluxDB measurement name. If it is not configured, the sink uses the input table name.        |
| username                    | string | no       | -                     | InfluxDB username. It must be configured together with `password`.                            |
| password                    | string | no       | -                     | InfluxDB password. It must be configured together with `username`.                            |
| key_time                    | string | no       | processing time       | Field name used as the InfluxDB point timestamp. If omitted, processing time is used.          |
| key_tags                    | array  | no       | -                     | Field names written as InfluxDB tags. Other fields are written as point fields.                |
| batch_size                  | int    | no       | 1024                  | Number of points buffered before flushing to InfluxDB.                                        |
| max_retries                 | int    | no       | -                     | Maximum retry count when flushing points fails.                                               |
| write_timeout               | int    | no       | 5                     | Write timeout used by the InfluxDB client.                                                     |
| retry_backoff_multiplier_ms | int    | no       | -                     | Backoff multiplier used between retry attempts, in milliseconds.                              |
| max_retry_backoff_ms        | int    | no       | -                     | Maximum backoff between retry attempts, in milliseconds.                                      |
| rp                          | string | no       | -                     | Retention policy used when writing points.                                                     |
| epoch                       | string | no       | n                     | Time precision used by the client. Uppercase values `H`, `M`, `S`, `MS`, `U`, and `NS` are recognized for write precision. |
| connect_timeout_ms          | long   | no       | 15000                 | Timeout for connecting to InfluxDB, in milliseconds.                                          |
| query_timeout_sec           | int    | no       | 3                     | Read timeout used by the InfluxDB client, in seconds.                                         |
| multi_table_sink_replica    | int    | no       | -                     | Replica count for multi-table sink writers.                                                   |
| common-options              | config | no       | -                     | Sink plugin common options.                                                                   |

### url

the url to connect to influxDB e.g.

```
http://influxdb-host:8086
```

### database [string]

The name of `influxDB` database

### measurement [string]

The name of `influxDB` measurement. This option is optional. If it is omitted, the sink uses the
input table full name as the measurement name, which is useful for multi-table writes.
For multi-table input, make sure the generated table names are valid InfluxDB measurement names.

### username [string]

`influxDB` user username

### password [string]

`influxDB` user password

### key_time [string]

Specify field-name of the `influxDB` measurement timestamp in SeaTunnelRow. If not specified, use processing-time as timestamp

### key_tags [array]

Specify field-name of the `influxDB` measurement tags in SeaTunnelRow.
If not specified, include all fields with `influxDB` measurement field

### batch_size [int]

For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `checkpoint.interval`, the data will be flushed into the influxDB

### max_retries [int]

The number of retries to flush failed

### retry_backoff_multiplier_ms [int]

Using as a multiplier for generating the next delay for backoff

### max_retry_backoff_ms [int]

The amount of time to wait before attempting to retry a request to `influxDB`

### write_timeout [int]

The write timeout used by the InfluxDB client.

### rp [string]

Retention policy used when writing points.

### epoch [string]

Time precision used by the InfluxDB client. For sink write precision, uppercase values `H`, `M`,
`S`, `MS`, `U`, and `NS` are recognized by the current connector. The default `n` is treated as
nanosecond precision.

### query_timeout_sec [int]

The read timeout used by the InfluxDB client, in seconds.

### connect_timeout_ms [long]

the timeout for connecting to InfluxDB, in milliseconds

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

## Examples

### Write One Measurement

```hocon
sink {
    InfluxDB {
        url = "http://influxdb-host:8086"
        database = "test"
        measurement = "sink"
        key_time = "time"
        key_tags = ["label"]
        batch_size = 1
    }
}

```

### Write Without Explicit Measurement

When `measurement` is omitted, the sink uses the input table full name as the measurement name.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      table = "influxdb_sink"
      fields {
        label = STRING
        c_string = STRING
        c_double = DOUBLE
        c_bigint = BIGINT
        c_float = FLOAT
        c_int = INT
        c_smallint = SMALLINT
        c_boolean = BOOLEAN
        time = BIGINT
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["label_1", "sink_1", 4.3, 200, 2.5, 2, 5, true, 1627529632356]
      }
    ]
  }
}

sink {
  InfluxDB {
    url = "http://influxdb-host:8086"
    database = "test"
    key_time = "time"
    key_tags = ["label"]
    batch_size = 1
  }
}
```

### Multiple Table

When `measurement` is omitted, each upstream table is written to a measurement named after that
table. This is the usual setting for multi-table input.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  InfluxDB {
    url = "http://influxdb-host:8086"
    database = "test"
    key_time = "time"
    batch_size = 1
  }
}
```

## Changelog

<ChangeLog />
