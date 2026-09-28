import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDB

> IoTDB sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to write data to IoTDB.

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

  > IoTDB supports the `exactly-once` feature through idempotent writing. If multiple data have the same `key` and `timestamp`, the latest one will overwrite the previous one.
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

> The IoTDB sink connector writes rows by calling the IoTDB insert RPC. When a row carries a non-unique `(device, timestamp)` pair, the write is treated as an upsert — the latest value overwrites earlier ones — so duplicate deliveries from upstream do not create phantom rows. Row-kind `UPDATE`/`DELETE` are not interpreted as CDC operations; all rows are written as inserts.

## Supported DataSource Info

| Datasource | Supported Versions           |      Url       |
|------------|------------------------------|----------------|
| IoTDB      | `0.13.0 <= version <= 1.3.X` | localhost:6667 |

## Data Type Mapping

| IotDB Data Type | SeaTunnel Data Type |
|-----------------|---------------------|
| BOOLEAN         | BOOLEAN             |
| INT32           | TINYINT             |
| INT32           | SMALLINT            |
| INT32           | INT                 |
| INT64           | BIGINT              |
| FLOAT           | FLOAT               |
| DOUBLE          | DOUBLE              |
| TEXT            | STRING              |

## Sink Options

| Name                        | Type    | Required | Default                        | Description                                                                                                                                                       |
|-----------------------------|---------|----------|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_urls                   | Array   | Yes      | -                              | IoTDB cluster address, the format is `["host1:port"]` or `["host1:port","host2:port"]`                                                                            |
| username                    | String  | Yes      | -                              | IoTDB user username                                                                                                                                               |
| password                    | String  | Yes      | -                              | IoTDB user password                                                                                                                                               |
| key_device                  | String  | Yes      | -                              | Specify field name of the IoTDB deviceId in SeaTunnelRow                                                                                                          |
| key_timestamp               | String  | No       | processing time                | Specify field-name of the IoTDB timestamp in SeaTunnelRow. If not specified, use processing-time as timestamp                                                     |
| key_measurement_fields      | Array   | No       | exclude device and timestamp fields | Specify field names of the IoTDB measurement list in SeaTunnelRow. If not specified, include all fields except `key_device` and `key_timestamp` fields.             |
| storage_group               | String  | No       | -                              | Specify device storage group(path prefix) <br/> example: deviceId = \${storage_group} + "." +  \${key_device}                                                     |
| batch_size                  | Integer | No       | 1024                           | For batch writing, data is flushed into IoTDB when the buffered row count reaches `batch_size`.                                                                    |
| max_retries                 | Integer | No       | -                              | The number of retries to flush failed                                                                                                                             |
| retry_backoff_multiplier_ms | Integer | No       | -                              | Using as a multiplier for generating the next delay for backoff                                                                                                   |
| max_retry_backoff_ms        | Integer | No       | -                              | The amount of time to wait before attempting to retry a request to `IoTDB`                                                                                        |
| default_thrift_buffer_size  | Integer | No       | -                              | Thrift init buffer size in IoTDB client                                                                                                                           |
| max_thrift_frame_size       | Integer | No       | -                              | Thrift max frame size in IoTDB client                                                                                                                             |
| zone_id                     | string  | No       | -                              | java.time.ZoneId in IoTDB client                                                                                                                                  |
| enable_rpc_compression      | Boolean | No       | -                              | Enable rpc compression in IoTDB client                                                                                                                            |
| connection_timeout_in_ms    | Integer | No       | -                              | The maximum time (in ms) to wait when connecting to IoTDB                                                                                                         |
| common-options              |         | no       | -                              | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details                                                       |

## Write Rules

- `key_device` must name the SeaTunnel field that contains the IoTDB device path.
- `storage_group` is a string prefix. When it is set, the final device path is built from `storage_group` and the value of `key_device`.
- `key_timestamp` can name a `STRING`, `BIGINT`, or `TIMESTAMP` field. If it is not configured, the connector uses the current processing time.
- If `key_measurement_fields` is not configured, all fields except `key_device` and `key_timestamp` are written as measurements.
- The sink supports `STRING`, `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, and `DOUBLE` measurement fields.

## Examples

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 16
    bigint.template = [1664035200001]
    schema = {
      fields {
        device_name = "string"
        temperature = "float"
        moisture = "int"
        event_ts = "bigint"
        c_string = "string"
        c_boolean = "boolean"
        c_tinyint = "tinyint"
        c_smallint = "smallint"
        c_int = "int"
        c_bigint = "bigint"
        c_float = "float"
        c_double = "double"
      }
    }
  }
}
```

The data format from upstream SeaTunnelRow is as follows:

|       device_name        | temperature | moisture |   event_ts    | c_string | c_boolean | c_tinyint | c_smallint | c_int |  c_bigint  | c_float | c_double |
|--------------------------|-------------|----------|---------------|----------|-----------|-----------|------------|-------|------------|---------|----------|
| root.test_group.device_a | 36.1        | 100      | 1664035200001 | abc1     | true      | 1         | 1          | 1     | 2147483648 | 1.0     | 1.0      |
| root.test_group.device_b | 36.2        | 101      | 1664035200001 | abc2     | false     | 2         | 2          | 2     | 2147483649 | 2.0     | 2.0      |
| root.test_group.device_c | 36.3        | 102      | 1664035200001 | abc3     | false     | 3         | 3          | 3     | 2147483649 | 3.0     | 3.0      |

### Case1

Only required options used:
- use current processing time as timestamp
- measurement fields include all fields excluding `key_device`

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    key_device = "device_name" # specify the `deviceId` use device_name field
  }
}
```

The data format of IoTDB output is as follows:

```shell
IoTDB> SELECT * FROM root.test_group.* align by device;
+------------------------+------------------------+--------------+-----------+--------------+---------+----------+----------+-----------+------+-----------+--------+---------+
|                    Time|                  Device|   temperature|   moisture|      event_ts| c_string| c_boolean| c_tinyint| c_smallint| c_int|   c_bigint| c_float| c_double|
+------------------------+------------------------+--------------+-----------+--------------+---------+----------+----------+-----------+------+-----------+--------+---------+
|2023-09-01T00:00:00.001Z|root.test_group.device_a|          36.1|        100| 1664035200001|     abc1|      true|         1|          1|     1| 2147483648|     1.0|      1.0| 
|2023-09-01T00:00:00.001Z|root.test_group.device_b|          36.2|        101| 1664035200001|     abc2|     false|         2|          2|     2| 2147483649|     2.0|      2.0|
|2023-09-01T00:00:00.001Z|root.test_group.device_c|          36.3|        102| 1664035200001|     abc2|     false|         3|          3|     3| 2147483649|     3.0|      3.0|
+------------------------+------------------------+--------------+-----------+--------------+---------+---------+-----------+-----------+------+-----------+--------+---------+
```

### Case2

Use source event's time:
- use `key_timestamp` as timestamp
- measurement fields include all fields excluding `key_device` & `key_timestamp`

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    key_device = "device_name" # specify the `deviceId` use device_name field
    key_timestamp = "event_ts" # specify the `timestamp` use event_ts field
  }
}
```

The data format of IoTDB output is as follows:

```shell
IoTDB> SELECT * FROM root.test_group.* align by device;
+------------------------+------------------------+--------------+-----------+--------------+---------+----------+----------+-----------+------+-----------+--------+---------+
|                    Time|                  Device|   temperature|   moisture|      event_ts| c_string| c_boolean| c_tinyint| c_smallint| c_int|   c_bigint| c_float| c_double|
+------------------------+------------------------+--------------+-----------+--------------+---------+----------+----------+-----------+------+-----------+--------+---------+
|2022-09-25T00:00:00.001Z|root.test_group.device_a|          36.1|        100| 1664035200001|     abc1|      true|         1|          1|     1| 2147483648|     1.0|      1.0| 
|2022-09-25T00:00:00.001Z|root.test_group.device_b|          36.2|        101| 1664035200001|     abc2|     false|         2|          2|     2| 2147483649|     2.0|      2.0|
|2022-09-25T00:00:00.001Z|root.test_group.device_c|          36.3|        102| 1664035200001|     abc2|     false|         3|          3|     3| 2147483649|     3.0|      3.0|
+------------------------+------------------------+--------------+-----------+--------------+---------+---------+-----------+-----------+------+-----------+--------+---------+
```

### Case3

Use source event's time and limit measurement fields:
- use `key_timestamp` as timestamp
- measurement fields include only fields specified in `key_measurement_fields`

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    key_device = "device_name"
    key_timestamp = "event_ts"
    key_measurement_fields = ["temperature", "moisture"]
  }
}
```

The data format of IoTDB output is as follows:

```shell
IoTDB> SELECT * FROM root.test_group.* align by device;
+------------------------+------------------------+--------------+-----------+
|                    Time|                  Device|   temperature|   moisture|
+------------------------+------------------------+--------------+-----------+
|2022-09-25T00:00:00.001Z|root.test_group.device_a|          36.1|        100|
|2022-09-25T00:00:00.001Z|root.test_group.device_b|          36.2|        101|
|2022-09-25T00:00:00.001Z|root.test_group.device_c|          36.3|        102|
+------------------------+------------------------+--------------+-----------+
```

### Case4: Streaming writes with explicit batch flush

For long-running streaming jobs, increase `batch_size` to reduce per-row RPC overhead. The connector flushes the buffered rows when either the buffer fills up to `batch_size` or the checkpoint completes. Set `max_retries` and `max_retry_backoff_ms` to keep the job resilient against transient RPC failures.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

sink {
  IoTDB {
    node_urls = ["localhost:6667", "localhost:6668"]
    username = "root"
    password = "root"
    key_device = "device_name"
    key_timestamp = "event_ts"
    batch_size = 2048
    max_retries = 3
    retry_backoff_multiplier_ms = 100
    max_retry_backoff_ms = 5000
  }
}
```

`node_urls` accepts multiple IoTDB nodes. The sink will pick one as the active write node per task and fall over to the others when the active node fails.

## Changelog

<ChangeLog />
