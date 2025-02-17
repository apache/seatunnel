# IoTDB

> IoTDB数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

将数据写入IoTDB。

## Using Dependency

### 适用于Spark/Flink引擎

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/org.apache.iotdb/iotdb-jdbc)已放置在目录`${SEATUNNEL_HOME}/plugins/`中。

### 适用于SeaTunnelZeta引擎

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/org.apache.iotdb/iotdb-jdbc)已放置在目录“${SEATUNNEL_HOME}/lib/”中。

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)

IoTDB通过幂等写支持 `exactly-once` 功能。如果两条数据
如果使用相同的`key`和`timestamp`，新数据将覆盖旧数据。

:::提示

IoTDB和Spark之间存在节俭版本冲突。因此，您需要执行`rm -f $SPARK_HOME/jars/libthrift*` 和 `cp $IOTDB_HOME/lib/libthrift* $SPARK_HOME/jars/`来解决这个问题。

:::

## 支持的数据源信息

| 数据源 | Supported 版本 |      地址       |
|------------|--------------------|----------------|
| IoTDB      | `>= 0.13.0`        | localhost:6667 |

## 数据类型映射

| IotDB 数据类型 | SeaTunnel 数据类型 |
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

|            名称             |  类型   | 是否必传 |            默认值             |                                                                            描述                                                                            |
|-----------------------------|---------|----------|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_urls                   | String  | 是      | -                              | `IoTDB` 集群地址，格式为 `"host1:port"` 或 `"host1:port,host2:port"`                                                                                |
| username                    | String  | 是      | -                              | `IoTDB` 用户的用户名                                                                                                                                             |
| password                    | String  | 是      | -                              | `IoTDB` 用户的密码                                                                                                                                             |
| key_device                  | String  | 是      | -                              | 在SeaTunnelRow中指定`IoTDB`设备ID的字段名                                                                                                        |
| key_timestamp               | String  | 否       | processing time                | 在SeaTunnelRow中指定`IoTDB`时间戳的字段名。如果未指定，则使用处理时间作为时间戳                                                   |
| key_measurement_fields      | Array   | 否       | exclude `device` & `timestamp` | 在SeaTunnelRow中指定`IoTDB`测量列表的字段名称。如果未指定，则包括所有字段，但排除 `device` & `timestamp`                       |
| storage_group               | Array   | 否       | -                              | 指定设备存储组（路径前缀） <br/> 例如: deviceId = ${storage_group} + "." +  ${key_device}                                                       |
| batch_size                  | Integer | 否       | 1024                           | 对于批写入，当缓冲区的数量达到`batch_size`的数量或时间达到`batch_interval_ms`时，数据将被刷新到IoTDB中 |
| max_retries                 | Integer | 否       | -                              | 刷新的重试次数 failed                                                                                                                             |
| retry_backoff_multiplier_ms | Integer | 否       | -                              | 用作生成下一个退避延迟的乘数                                                                                                   |
| max_retry_backoff_ms        | Integer | 否       | -                              | 尝试重试对`IoTDB`的请求之前等待的时间量                                                                                        |
| default_thrift_buffer_size  | Integer | 否       | -                              | 在`IoTDB`客户端中节省初始化缓冲区大小                                                                                                                         |
| max_thrift_frame_size       | Integer | 否       | -                              | 在`IoTDB`客户端中节约最大帧大小                                                                                                                           |
| zone_id                     | string  | 否       | -                              | `IoTDB` java.time.ZoneId  client                                                                                                                                |
| enable_rpc_compression      | Boolean | 否       | -                              | 在`IoTDB`客户端中启用rpc压缩                                                                                                                          |
| connection_timeout_in_ms    | Integer | 否       | -                              | 连接到`IoTDB`时等待的最长时间（毫秒）                                                                                                       |
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
使用当前处理时间作为时间戳。并包括所有字段，但不包括`device` & `timestamp`作为测量字段

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

"IoTDB"数据格式的输出如下:

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

"IoTDB"数据格式的输出如下:

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

"IoTDB"数据格式的输出如下:

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

## 更改日志

### 2.2.0-beta 2022-09-26

- 添加IoTDB数据接收器

### 2.3.0-beta 2022-10-20

- [Improve] 改进IoTDB数据接收器 ([2917](https://github.com/apache/seatunnel/pull/2917))
  - 支持sql语法对齐
  - 支持sql拆分忽略案例
  - 支持将拆分偏移量恢复到至少一次
  - 支持从RowRecord读取时间戳
- [BugFix] 固定IoTDB连接器写入NPE ([3080](https://github.com/apache/seatunnel/pull/3080))

