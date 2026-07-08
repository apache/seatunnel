import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDB

> IoTDB数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于将数据写入 IoTDB。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  > IoTDB 通过幂等写支持`精确一次`功能。如果两条数据使用相同的`key`和`timestamp`，新数据将覆盖旧数据。
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源   | 支持的版本                        | 地址             |
|-------|------------------------------|----------------|
| IoTDB | `0.13.0 <= version <= 1.3.X` | localhost:6667 |

## 数据类型映射

| IoTDB 数据类型 | SeaTunnel 数据类型 |
|------------|----------------|
| BOOLEAN    | BOOLEAN        |
| INT32      | TINYINT        |
| INT32      | SMALLINT       |
| INT32      | INT            |
| INT64      | BIGINT         |
| FLOAT      | FLOAT          |
| DOUBLE     | DOUBLE         |
| TEXT       | STRING         |

## Sink 选项

| 名称                          | 类型      | 是否必传 | 默认值                            | 描述                                                                           |
|-----------------------------|---------|------|--------------------------------|------------------------------------------------------------------------------|
| node_urls                   | Array   | 是    | -                              | IoTDB 集群地址，格式为 `["host1:port"]` 或 `["host1:port","host2:port"]`              |
| username                    | String  | 是    | -                              | IoTDB 用户的用户名                                                                 |
| password                    | String  | 是    | -                              | IoTDB 用户的密码                                                                  |
| key_device                  | String  | 是    | -                              | 在SeaTunnelRow中指定 IoTDB 设备ID的字段名                                              |
| key_timestamp               | String  | 否    | processing time                | 在SeaTunnelRow中指定 IoTDB 时间戳的字段名。如果未指定，则使用处理时间作为时间戳                            |
| key_measurement_fields      | Array   | 否    | 排除设备和时间字段                       | 在 SeaTunnelRow 中指定 IoTDB 测点字段列表。未配置时，会写入除 `key_device` 和 `key_timestamp` 对应字段外的其他字段 |
| storage_group               | String  | 否    | -                              | 指定设备存储组（路径前缀） <br/> 例如: deviceId = \${storage_group} + "." +  \${key_device} |
| batch_size                  | Integer | 否    | 1024                           | 批量写入时，当缓存行数达到 `batch_size`，数据会刷新到 IoTDB 中                                      |
| max_retries                 | Integer | 否    | -                              | 写入失败后的最大重试次数                                                                 |
| retry_backoff_multiplier_ms | Integer | 否    | -                              | 用作生成下一个退避延迟的乘数                                                               |
| max_retry_backoff_ms        | Integer | 否    | -                              | 尝试重试对 IoTDB 的请求之前等待的时间量                                                      |
| default_thrift_buffer_size  | Integer | 否    | -                              | IoTDB 客户端的初始化 Thrift 缓冲区大小                                                   |
| max_thrift_frame_size       | Integer | 否    | -                              | IoTDB 客户端的最大 Thrift 帧大小                                                       |
| zone_id                     | string  | 否    | -                              | IoTDB 客户端使用的 `java.time.ZoneId`                                                |
| enable_rpc_compression      | Boolean | 否    | -                              | 在 IoTDB 客户端中启用rpc压缩                                                          |
| connection_timeout_in_ms    | Integer | 否    | -                              | 连接到 IoTDB 时等待的最长时间（毫秒）                                                       |
| common-options              |         | 否    | -                              | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)              |

## 写入规则

- `key_device` 必须指定 SeaTunnel 中保存 IoTDB 设备路径的字段名。
- `storage_group` 是字符串前缀。配置后，最终设备路径会由 `storage_group` 和 `key_device` 对应字段值拼接而成。
- `key_timestamp` 可以指定 `STRING`、`BIGINT` 或 `TIMESTAMP` 类型字段。未配置时，连接器会使用当前处理时间。
- 未配置 `key_measurement_fields` 时，会把除 `key_device` 和 `key_timestamp` 之外的所有字段写为测点。
- sink 支持写入 `STRING`、`BOOLEAN`、`TINYINT`、`SMALLINT`、`INT`、`BIGINT`、`FLOAT` 和 `DOUBLE` 类型测点。

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

只填写所需的配置：
- 使用当前处理时间作为时间戳
- 测点包括排除了`key_device`后的其余字段

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    key_device = "device_name" # 使用 device_name 字段作为 deviceId
  }
}
```

IoTDB 数据格式的输出如下:

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

使用源事件的时间：
- 使用指定字段作为时间戳
- 测点包括排除了`key_device`和`key_timestamp`后的其余字段

```hocon
sink {
  IoTDB {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    key_device = "device_name" # 使用 device_name 字段作为 deviceId
    key_timestamp = "event_ts" # 使用 event_ts 字段作为 timestamp
  }
}
```

IoTDB 数据格式的输出如下:

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

使用源事件的时间和限制测量字段：
- 使用指定字段作为时间戳
- 测点仅包括`key_measurement_fields`指定的字段

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

IoTDB 数据格式的输出如下:

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

## 变更日志

<ChangeLog />
