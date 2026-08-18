import ChangeLog from '../changelog/connector-influxdb.md';

# InfluxDB

> InfluxDB sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write SeaTunnel rows to InfluxDB 1.x. The sink converts one row into one InfluxDB point, using
`measurement`, `key_time`, and `key_tags` to decide the target measurement, timestamp, tags, and
fields.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

| SeaTunnel Data Type | InfluxDB Usage |
|---------------------|----------------|
| BOOLEAN             | Written as an InfluxDB field. |
| SMALLINT            | Written as an InfluxDB field. |
| INT                 | Written as an InfluxDB field. |
| BIGINT              | Written as an InfluxDB field, or as the timestamp when configured by `key_time`. |
| FLOAT               | Written as an InfluxDB field. |
| DOUBLE              | Written as an InfluxDB field. |
| STRING              | Written as an InfluxDB field, a tag value, or a timestamp string when configured by `key_time`. |
| TIMESTAMP           | Can be used by `key_time`; converted with UTC zone to epoch milliseconds. |

Other SeaTunnel types are not supported by the current InfluxDB sink serializer.

## Sink Options

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
| common-options              | config | no       | -                     | Sink plugin common options. See [Sink Common Options](../common-options/sink-common-options.md). |

### url [string]

The URL to connect to InfluxDB, for example `http://influxdb-host:8086`.

### database [string]

The name of the InfluxDB database that points are written to.

### measurement [string]

The InfluxDB measurement name. When omitted, the sink uses the input table full name as the
measurement — which is the common setting for multi-table writes. Make sure the generated
table names are valid InfluxDB measurement names.

### username [string]

The InfluxDB user name. Configure it together with `password` when the influxdb requires authentication.

### password [string]

The InfluxDB user password. Configure it together with `username` when authentication is required.

### key_time [string]

The field name in the upstream row that supplies the InfluxDB point timestamp. When omitted, the
sink uses the current processing time. Values configured here are accepted as numeric timestamps
or as an ISO-8601 timestamp string.

### key_tags [array]

The field names in the upstream row that should be written as InfluxDB tags. All other fields are
written as point fields. When omitted, every field is written as a point field.

### batch_size [int]

Number of points buffered before flushing to InfluxDB. The default is `1024`. The buffer is also
flushed at each checkpoint and when the writer closes.

### max_retries [int]

Maximum retry count when the flush fails. When this option is not set, the sink writes once and
fails immediately if that write fails.

### retry_backoff_multiplier_ms [int]

Backoff multiplier used between retry attempts, in milliseconds. Configure it together with
`max_retry_backoff_ms` so the retry sleep interval is non-zero.

### max_retry_backoff_ms [int]

Maximum backoff between retry attempts, in milliseconds. Configure it together with
`retry_backoff_multiplier_ms` so the retry sleep interval is non-zero.

### write_timeout [int]

The write timeout used by the InfluxDB client, in seconds.

### rp [string]

Retention policy used when writing points.

### epoch [string]

Time precision used by the InfluxDB client. Uppercase values `H`, `M`, `S`, `MS`, `U`, and `NS`
are recognized for write precision. The default value `n` is treated as nanosecond precision.

### query_timeout_sec [int]

The read timeout used by the InfluxDB client, in seconds.

### connect_timeout_ms [long]

The timeout for connecting to InfluxDB, in milliseconds. The default is `15000`.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Write One Measurement

A simple sink that writes rows to a single named measurement.

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

### Multi-Table Write

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
