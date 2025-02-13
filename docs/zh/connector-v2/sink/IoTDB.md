# IoTDB

> IoTDB接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

Used to write data to IoTDB.

## Using Dependency

### 适用于Spark/Flink发动机

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/org.apache.iotdb/iotdb-jdbc)已放置在目录`${SEATUNNEL_HOME}/plugins/`中。

### 适用于海底隧道Zeta发动机

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/org.apache.iotdb/iotdb-jdbc)已放置在目录“${SEATUNNEL_HOME}/lib/”中。

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)

IoTDB通过幂等写支持“精确一次”功能。如果两条数据
如果使用相同的“密钥”和“时间戳”，新数据将覆盖旧数据。

:::tip

IoTDB和Spark之间存在节俭版本冲突。因此，您需要执行“rm-f$Spark_HOME/gars/libthrift*”和“cp$IoTDB_HOME/lib/libthrifd*$SPARKHOME/gars/”来解决这个问题。

:::

## 支持的数据源信息

| Datasource | Supported Versions |      Url       |
|------------|--------------------|----------------|
| IoTDB      | `>= 0.13.0`        | localhost:6667 |

## 数据类型映射

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

## Sink 选项

|            名称             |  类型   | 需要 |            默认             |                                                                            描述                                                                            |
|-----------------------------|---------|----------|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_urls                   | String  | 是      | -                              | `IoTDB` cluster address, the format is `"host1:port"` or `"host1:port,host2:port"`                                                                                |
| username                    | String  | 是      | -                              | `IoTDB` user username                                                                                                                                             |
| password                    | String  | 是      | -                              | `IoTDB` user password                                                                                                                                             |
| key_device                  | String  | 是      | -                              | Specify field name of the `IoTDB` deviceId in SeaTunnelRow                                                                                                        |
| key_timestamp               | String  | 否       | processing time                | Specify field-name of the `IoTDB` timestamp in SeaTunnelRow. If not specified, use processing-time as timestamp                                                   |
| key_measurement_fields      | Array   | 否       | exclude `device` & `timestamp` | Specify field-name of the `IoTDB` measurement list in SeaTunnelRow. If not specified, include all fields but exclude `device` & `timestamp`                       |
| storage_group               | Array   | 否       | -                              | Specify device storage group(path prefix) <br/> example: deviceId = ${storage_group} + "." +  ${key_device}                                                       |
| batch_size                  | Integer | 否       | 1024                           | For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the IoTDB |
| max_retries                 | Integer | 否       | -                              | The number of retries to flush failed                                                                                                                             |
| retry_backoff_multiplier_ms | Integer | 否       | -                              | Using as a multiplier for generating the next delay for backoff                                                                                                   |
| max_retry_backoff_ms        | Integer | 否       | -                              | The amount of time to wait before attempting to retry a request to `IoTDB`                                                                                        |
| default_thrift_buffer_size  | Integer | 否       | -                              | Thrift init buffer size in `IoTDB` client                                                                                                                         |
| max_thrift_frame_size       | Integer | 否       | -                              | Thrift max frame size in `IoTDB` client                                                                                                                           |
| zone_id                     | string  | 否       | -                              | java.time.ZoneId in `IoTDB` client                                                                                                                                |
| enable_rpc_compression      | Boolean | 否       | -                              | Enable rpc compression in `IoTDB` client                                                                                                                          |
| connection_timeout_in_ms    | Integer | 否       | -                              | The maximum time (in ms) to wait when connecting to `IoTDB`                                                                                                       |
| common-options              |         | 否       | -                              | Sink插件常用参数，详见[Sink common Options]（../Sink common Options.md）                                        |

## 示例

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

上游SeaTunnelRow数据格式如下:

|       device_name        | temperature | moisture |   event_ts    | c_string | c_boolean | c_tinyint | c_smallint | c_int |  c_bigint  | c_float | c_double |
|--------------------------|-------------|----------|---------------|----------|-----------|-----------|------------|-------|------------|---------|----------|
| root.test_group.device_a | 36.1        | 100      | 1664035200001 | abc1     | true      | 1         | 1          | 1     | 2147483648 | 1.0     | 1.0      |
| root.test_group.device_b | 36.2        | 101      | 1664035200001 | abc2     | false     | 2         | 2          | 2     | 2147483649 | 2.0     | 2.0      |
| root.test_group.device_c | 36.3        | 102      | 1664035200001 | abc3     | false     | 3         | 3          | 3     | 2147483649 | 3.0     | 3.0      |

### 案例1

只填写所需的配置。
使用当前处理时间作为时间戳。并包括所有字段，但不包括“设备”和“时间戳”作为测量字段

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

“IoTDB”数据格式的输出如下:

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

### 案例2

使用源事件的时间

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

“IoTDB”数据格式的输出如下:

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

### 案例3

使用源事件的时间和限制度量字段

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

“IoTDB”数据格式的输出如下:

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

- 添加IoTDB接收器连接器

### 2.3.0-beta 2022-10-20

- [Improve] 改进IoTDB接收器连接器 ([2917](https://github.com/apache/seatunnel/pull/2917))
  - 支持sql语法对齐
  - 支持sql拆分忽略案例
  - 支持将拆分偏移量恢复到至少一次
  - 支持从RowRecord读取时间戳
- [BugFix] 固定IoTDB连接器槽NPE ([3080](https://github.com/apache/seatunnel/pull/3080))

