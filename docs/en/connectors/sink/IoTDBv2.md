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

- [x] [exactly-once](../../concept/connector-v2-features.md)

    > IoTDB supports the `exactly-once` feature through idempotent writing. If multiple data have the same `key` and `timestamp`, the latest one will overwrite the previous one.
  
## Supported DataSource Info

| Datasource | Supported Versions |      Url       |
|------------|--------------------|----------------|
| IoTDB      | `2.0 <= version`   | localhost:6667 |

## Data Type Mapping

| SeaTunnel Data Type | IoTDB Data Type | 
|---------------------|-----------------|
| BOOLEAN             | BOOLEAN         |
| TINYINT             | INT32           |
| SMALLINT            | INT32           |
| INT                 | INT32           |
| BIGINT              | INT64           |
| FLOAT               | FLOAT           |
| DOUBLE              | DOUBLE          |
| STRING              | STRING          |
| TIMESTAMP           | TIMESTAMP       |
| DATE                | DATE            |

## Sink Options

| Name                        | Type    | Required | Default              | Description                                                                                                                                                                                                                                                                                                                                                                |
|-----------------------------|---------|----------|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_urls                   | Array   | Yes      | -                    | IoTDB cluster address, the format is `["host1:port"]` or `["host1:port","host2:port"]`                                                                                                                                                                                                                                                                                     |
| username                    | String  | Yes      | -                    | IoTDB username                                                                                                                                                                                                                                                                                                                                                             |
| password                    | String  | Yes      | -                    | IoTDB user password                                                                                                                                                                                                                                                                                                                                                        |
| sql_dialect                 | String  | No       | tree                 | the sql dialect of IoTDB, options available is `"tree"` or `"table"`                                                                                                                                                                                                                                                                                                       |
| storage_group               | String  | Yes      | -                    | IoTDB-tree: Specify the device storage group(path prefix) <br/> example: deviceId = \${storage_group} + "." +  \${key_device} <br/> IoTDB-table: Specify the database                                                                                                                                                                                                      |
| key_device                  | String  | Yes      | -                    | IoTDB-tree: Specify the field name in SeaTunnelRow to be used as device id <br/> IoTDB-table: Specify the field name in SeaTunnelRow to be used as table name                                                                                                                                                                                                              |
| key_timestamp               | String  | No       | processing time      | IoTDB-tree: Specify the field name in SeaTunnelRow to be used as timestamp (processing time will be used by default) <br/> IoTDB-table: Specify the field name in SeaTunnelRow to be used as time column (processing time will be used by default)                                                                                                                         |
| key_measurement_fields      | Array   | No       | refer to description | IoTDB-tree: Specify the field names in SeaTunnelRow to be used as measurement (all fields excluding `key_device`&`key_timestamp` will be used by default) <br/> IoTDB-table: Specify the field names in SeaTunnelRow to be used as FIELD columns (all fields excluding `key_device`, `key_timestamp`, `key_tag_fields` and `key_attribute_fields` will be used by default) |
| key_tag_fields              | Array   | No       | -                    | IoTDB-tree: invalid <br/> IoTDB-table: Specify the field names in SeaTunnelRow to be used as TAG columns                                                                                                                                                                                                                                                                   |
| key_attribute_fields        | Array   | No       | -                    | IoTDB-tree: invalid <br/> IoTDB-table: Specify the field names in SeaTunnelRow to be used as ATTRIBUTE columns                                                                                                                                                                                                                                                             |
| batch_size                  | Integer | No       | 1024                 | In batch writing, the data will be flushed into the IoTDB either when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`                                                                                                                                                                                                     |
| max_retries                 | Integer | No       | -                    | The number of times retrying to flush                                                                                                                                                                                                                                                                                                                                      |
| retry_backoff_multiplier_ms | Integer | No       | -                    | Used as a multiplier for generating the next delay for backoff                                                                                                                                                                                                                                                                                                             |
| max_retry_backoff_ms        | Integer | No       | -                    | The amount of time to wait before attempting to retry a request to IoTDB                                                                                                                                                                                                                                                                                                   |
| default_thrift_buffer_size  | Integer | No       | -                    | Thrift init buffer size in IoTDB client                                                                                                                                                                                                                                                                                                                                    |
| max_thrift_frame_size       | Integer | No       | -                    | Thrift max frame size in IoTDB client                                                                                                                                                                                                                                                                                                                                      |
| zone_id                     | String  | No       | -                    | java.time.ZoneId in IoTDB client                                                                                                                                                                                                                                                                                                                                           |
| enable_rpc_compression      | Boolean | No       | -                    | Enable rpc compression in IoTDB client, only valid in IoTDB-tree                                                                                                                                                                                                                                                                                                           |
| connection_timeout_in_ms    | Integer | No       | -                    | The maximum time (in ms) to wait when connecting to IoTDB                                                                                                                                                                                                                                                                                                                  |
| common-options              |         | no       | -                    | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                                                                                                                                                |

## Examples

### Example 1: Write data to IoTDB-tree

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

#### Case 1

Only required options used:
- use current processing time as timestamp
- measurement fields include all fields excluding `key_device`

```hocon
sink {
  IoTDB {
    node_urls = "localhost:6667"
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

#### Case 2

Use source event's time:
- use `key_timestamp` as timestamp
- measurement fields include all fields excluding `key_device` & `key_timestamp`

```hocon
sink {
  IoTDB {
    node_urls = "localhost:6667"
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

#### Case 3

Use source event's time and limit measurement fields:
- use `key_timestamp` as timestamp
- measurement fields include only fields specified in `key_measurement_fields`

```hocon
sink {
  IoTDB {
    node_urls = "localhost:6667"
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

### Example 2： Write data into IoTDB-table

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    ...
    schema = {
      fields {
        ts = timestamp
        model_id = string
        region = string
        tag = string
        status = boolean
        arrival_date = date
        temperature = double
      }
    }
  }
}
```

The data format from upstream SeaTunnelRow is as follows:

| ts                      | model_id | region | tag  | status | arrival_date | temperature |
|-------------------------|----------|--------|------|--------|--------------|-------------|
| 2025-07-30T17:52:34.851 | id1      | 0700HK | tag1 | true   | 2024-11-12   | 4.34        |
| 2025-07-29T17:51:34.851 | id2      | 0700HK | tag2 | false  | 2024-12-01   | 5.54        |
| 2025-07-28T17:50:34.851 | id3      | 0700HK | tag3 | false  | 2024-12-22   | 7.34        |

#### Case 1

Only required options used:
- use current processing time as timestamp
- FIELD columns include all fields excluding `key_device`

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    sql_dialect = "table"
    storage_group = "test_database"
    key_device = "region" 
  }
}
```

The data format of IoTDB output is as follows:

```shell
IoTDB> SELECT * FROM "test_database"."0700HK";
+-----------------------------+-----------------------+--------+----+------+------------+-----------+
|                         time|                     ts|model_id| tag|status|arrival_date|temperature|
+-----------------------------+-----------------------+--------+----+------+------------+-----------+
|2025-08-14T17:52:34.851+08:00|2025-07-30T17:52:34.851|     id1|tag1|  true|  2024-11-12|       4.34|
|2025-08-14T17:51:34.851+08:00|2025-07-29T17:51:34.851|     id2|tag2| false|  2024-12-01|       5.54|
|2025-08-14T17:50:34.851+08:00|2025-07-28T17:50:34.851|     id3|tag3| false|  2024-12-22|       7.34|
+-----------------------------+-----------------------+--------+----+------+------------+-----------+
```
```shell
IoTDB> DESC "test_database"."0700HK";
+------------+---------+--------+
|  ColumnName| DataType|Category|
+------------+---------+--------+
|        time|TIMESTAMP|    TIME|
|          ts|TIMESTAMP|   FIELD|
|    model_id|   STRING|   FIELD|
|         tag|   STRING|   FIELD|
|      status|  BOOLEAN|   FIELD|
|arrival_date|     DATE|   FIELD|
| temperature|   DOUBLE|   FIELD|
+------------+---------+--------+
```

#### Case 2

Use source event's time and limit TAG and ATTRIBUTE columns:
- use `key_timestamp` as time column
- use specified fields as TAG columns and ATTRIBUTE columns
- FIELD columns include all fields excluding `key_device`,`key_timestamp`,`key_tag_fields`and`key_attribute_fields`

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    sql_dialect = "table"
    storage_group = "test_database"
    key_device = "region" 
    key_timestamp = "ts"
    key_tag_fields = ["tag"]
    key_attribute_fields = ["model_id"]
  }
}
```

The data format of IoTDB output is as follows:

```shell
IoTDB> SELECT * FROM "test_database"."0700HK";
+-----------------------------+----+--------+------+------------+-----------+
|                         time| tag|model_id|status|arrival_date|temperature|
+-----------------------------+----+--------+------+------------+-----------+
|2025-07-30T17:52:34.851+08:00|tag1|     id1|  true|  2024-11-12|       4.34|
|2025-07-29T17:51:34.851+08:00|tag2|     id2| false|  2024-12-01|       5.54|
|2025-07-28T17:50:34.851+08:00|tag3|     id3| false|  2024-12-22|       7.34|
+-----------------------------+----+--------+------+------------+-----------+
```
```shell
IoTDB> DESC "test_database"."0700HK";
+------------+---------+---------+
|  ColumnName| DataType| Category|
+------------+---------+---------+
|        time|TIMESTAMP|     TIME|
|         tag|   STRING|      TAG|
|    model_id|   STRING|ATTRIBUTE|
|      status|  BOOLEAN|    FIELD|
|arrival_date|     DATE|    FIELD|
| temperature|   DOUBLE|    FIELD|
+------------+---------+---------+
```

#### Case 3

Use source event's time and limit FIELD columns:
- use `key_timestamp` as time column
- use specified fields as FIELD columns

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    sql_dialect = "table"
    storage_group = "test_database"
    key_device = "region" 
    key_timestamp = "ts"
    key_measurement_fields = ["status", "temperature"]
  }
}
```

The data format of IoTDB output is as follows:

```shell
IoTDB> SELECT * FROM "test_database"."0700HK";
+-----------------------------+------+-----------+
|                         time|status|temperature|
+-----------------------------+------+-----------+
|2025-07-30T17:52:34.851+08:00|  true|       4.34|
|2025-07-29T17:51:34.851+08:00| false|       5.54|
|2025-07-28T17:50:34.851+08:00| false|       7.34|
+-----------------------------+------+-----------+
```
```shell
IoTDB> DESC "test_database"."0700HK";
+-----------+---------+--------+
| ColumnName| DataType|Category|
+-----------+---------+--------+
|       time|TIMESTAMP|    TIME|
|     status|  BOOLEAN|   FIELD|
|temperature|   DOUBLE|   FIELD|
+-----------+---------+-------+
```

## Changelog

<ChangeLog />
