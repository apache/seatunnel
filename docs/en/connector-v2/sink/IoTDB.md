# IoTDB

> IoTDB sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)

IoTDB supports the `exactly-once` feature through idempotent writing. If two pieces of data have
the same `key` and `timestamp`, the new data will overwrite the old one.

## Description

Used to write data to IoTDB.

:::tip

There is a conflict of thrift version between IoTDB and Spark.Therefore, you need to execute `rm -f $SPARK_HOME/jars/libthrift*` and `cp $IOTDB_HOME/lib/libthrift* $SPARK_HOME/jars/` to resolve it.

:::

## Supported DataSource Info

| Datasource | Supported Versions |      Url       |
|------------|--------------------|----------------|
| IoTDB      | `>= 0.13.0`        | localhost:6667 |

## Database Dependency

## Data Type Mapping

| IotDB Data type | SeaTunnel Data type |
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

|            Name             |  Type   | Required |            Default             |                                                                            Description                                                                            |
|-----------------------------|---------|----------|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_urls                   | Array   | Yes      | -                              | `IoTDB` cluster address, the format is `["host:port", ...]`                                                                                                       |
| username                    | String  | Yes      | -                              | `IoTDB` user username                                                                                                                                             |
| password                    | String  | Yes      | -                              | `IoTDB` user password                                                                                                                                             |
| key_device                  | String  | No       | -                              | Specify field name of the `IoTDB` deviceId in SeaTunnelRow                                                                                                        |
| key_timestamp               | String  | No       | processing time                | Specify field-name of the `IoTDB` timestamp in SeaTunnelRow. If not specified, use processing-time as timestamp                                                   |
| key_measurement_fields      | Array   | No       | exclude `device` & `timestamp` | Specify field-name of the `IoTDB` measurement list in SeaTunnelRow. If not specified, include all fields but exclude `device` & `timestamp`                       |
| storage_group               | Array   | No       | -                              | Specify device storage group(path prefix) <br/> example: deviceId = ${storage_group} + "." +  ${key_device}                                                       |
| batch_size                  | Integer | No       | 1024                           | For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the IoTDB |
| max_retries                 | Integer | No       | -                              | The number of retries to flush failed                                                                                                                             |
| retry_backoff_multiplier_ms | Integer | No       | -                              | Using as a multiplier for generating the next delay for backoff                                                                                                   |
| max_retry_backoff_ms        | Integer | No       | -                              | The amount of time to wait before attempting to retry a request to `IoTDB`                                                                                        |
| default_thrift_buffer_size  | Integer | No       | -                              | Thrift init buffer size in `IoTDB` client                                                                                                                         |
| max_thrift_frame_size       | Integer | No       | -                              | Thrift max frame size in `IoTDB` client                                                                                                                           |
| zone_id                     | string  | No       | -                              | java.time.ZoneId in `IoTDB` client                                                                                                                                |
| enable_rpc_compression      | Boolean | No       | -                              | Enable rpc compression in `IoTDB` client                                                                                                                          |
| connection_timeout_in_ms    | Integer | No       | -                              | The maximum time (in ms) to wait when connecting to `IoTDB`                                                                                                       |
| common-options              |         | no       | -                              | Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details                                                               |

## Task Example

### Case1

Common options:

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    batch_size = 1024
  }
}
```

When you assign `key_device`  is `device_name`, for example:

```hocon
sink {
  IoTDB {
    ...
    key_device = "device_name"
  }
}
```

Upstream SeaTunnelRow data format is the following:

|       device_name        | field_1 | field_2 |
|--------------------------|---------|---------|
| root.test_group.device_a | 1001    | 1002    |
| root.test_group.device_b | 2001    | 2002    |
| root.test_group.device_c | 3001    | 3002    |

Output to `IoTDB` data format is the following:

```shell
IoTDB> SELECT * FROM root.test_group.* align by device;
+------------------------+------------------------+-----------+----------+
|                    Time|                  Device|   field_1|    field_2|
+------------------------+------------------------+----------+-----------+
|2022-09-26T17:50:01.201Z|root.test_group.device_a|      1001|       1002|
|2022-09-26T17:50:01.202Z|root.test_group.device_b|      2001|       2002|
|2022-09-26T17:50:01.203Z|root.test_group.device_c|      3001|       3002|
+------------------------+------------------------+----------+-----------+
```

### Case2

When you assign `key_device`、`key_timestamp`、`key_measurement_fields`, for example:

```hocon
sink {
  IoTDB {
    ...
    key_device = "device_name"
    key_timestamp = "ts"
    key_measurement_fields = ["temperature", "moisture"]
  }
}
```

Upstream SeaTunnelRow data format is the following:

|      ts       |       device_name        | field_1 | field_2 | temperature | moisture |
|---------------|--------------------------|---------|---------|-------------|----------|
| 1664035200001 | root.test_group.device_a | 1001    | 1002    | 36.1        | 100      |
| 1664035200001 | root.test_group.device_b | 2001    | 2002    | 36.2        | 101      |
| 1664035200001 | root.test_group.device_c | 3001    | 3002    | 36.3        | 102      |

Output to `IoTDB` data format is the following:

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

## Changelog

### 2.2.0-beta 2022-09-26

- Add IoTDB Sink Connector

### 2.3.0-beta 2022-10-20

- [Improve] Improve IoTDB Sink Connector ([2917](https://github.com/apache/seatunnel/pull/2917))
  - Support align by sql syntax
  - Support sql split ignore case
  - Support restore split offset to at-least-once
  - Support read timestamp from RowRecord
- [BugFix] Fix IoTDB connector sink NPE ([3080](https://github.com/apache/seatunnel/pull/3080))

