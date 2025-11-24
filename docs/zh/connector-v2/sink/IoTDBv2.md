import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDB

> IoTDB 数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于将数据写入 IoTDB。

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)

    > IoTDB 通过幂等写支持`精确一次`功能。如果两条数据使用相同的`key`和`timestamp`，新数据将覆盖旧数据。
  
## 支持的数据源信息

| 数据源   | 支持的版本            | 地址             |
|-------|------------------|----------------|
| IoTDB | `2.0 <= version` | localhost:6667 |

## 数据类型映射

| SeaTunnel 数据类型 | IoTDB 数据类型 | 
|----------------|------------|
| BOOLEAN        | BOOLEAN    |
| TINYINT        | INT32      |
| SMALLINT       | INT32      |
| INT            | INT32      |
| BIGINT         | INT64      |
| FLOAT          | FLOAT      |
| DOUBLE         | DOUBLE     |
| STRING         | STRING     |
| TIMESTAMP      | TIMESTAMP  |
| DATE           | DATE       |

## Sink 选项

| 名称                          | 类型      | 是否必填 | 默认值    | 描述                                                                                                                                                                                                                                      |
|-----------------------------|---------|------|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| node_urls                   | Array   | 是    | -      | IoTDB 集群地址，格式为 `["host1:port"]` 或 `["host1:port","host2:port"]`                                                                                                                                                                         |
| username                    | String  | 是    | -      | IoTDB 用户名                                                                                                                                                                                                                               |
| password                    | String  | 是    | -      | IoTDB 用户密码                                                                                                                                                                                                                              |
| sql_dialect                 | String  | 否    | tree   | IoTDB 模型，tree：树模型；table：表模型                                                                                                                                                                                                             |
| storage_group               | String  | 是    | -      | IoTDB 树模型：指定设备存储组（路径前缀） <br/> 例如: deviceId = \${storage_group} + "." +  \${key_device} <br/> IoTDB 表模型：指定数据库                                                                                                                            |
| key_device                  | String  | 是    | -      | IoTDB 树模型：在 SeaTunnelRow 中指定 IoTDB 设备 ID 的字段名；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 表名的字段名                                                                                                                                           |
| key_timestamp               | String  | 否    | 数据处理时间 | IoTDB 树模型：在 SeaTunnelRow 中指定 IoTDB 时间戳的字段名（如未指定，则使用处理时间作为时间戳）；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 时间列的字段名（如未指定，则使用处理时间作为时间戳）                                                                                                       |
| key_measurement_fields      | Array   | 否    | 见描述    | IoTDB 树模型：在 SeaTunnelRow 中指定 IoTDB 测量列表的字段名（如未指定，则包括排除`key_device`&`key_timestamp`后的其余字段）；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 测点列（FIELD）的字段名（如未指定，则包括排除`key_device`&`key_timestamp`&`key_tag_fields`&`key_attribute_fields`后的其余字段） |
| key_tag_fields              | Array   | 否    | -      | IoTDB 树模型：不生效；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 标签列（TAG）的字段名                                                                                                                                                                     |
| key_attribute_fields        | Array   | 否    | -      | IoTDB 树模型：不生效；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 属性列（ATTRIBUTE）的字段名                                                                                                                                                               |
| batch_size                  | Integer | 否    | 1024   | 对于批写入，当缓冲区的数量达到`batch_size`的数量或时间达到`batch_interval_ms`时，数据将被刷新到 IoTDB 中                                                                                                                                                                 |
| max_retries                 | Integer | 否    | -      | 刷新的重试次数                                                                                                                                                                                                                                 |
| retry_backoff_multiplier_ms | Integer | 否    | -      | 用作生成下一个退避延迟的乘数                                                                                                                                                                                                                          |
| max_retry_backoff_ms        | Integer | 否    | -      | 尝试重试对 IoTDB 的请求之前等待的时间量                                                                                                                                                                                                                 |
| default_thrift_buffer_size  | Integer | 否    | -      | 在 IoTDB 客户端中节省初始化缓冲区大小                                                                                                                                                                                                                  |
| max_thrift_frame_size       | Integer | 否    | -      | 在 IoTDB 客户端中节约最大帧大小                                                                                                                                                                                                                     |
| zone_id                     | String  | 否    | -      | IoTDB java.time.ZoneId  client                                                                                                                                                                                                          |
| enable_rpc_compression      | Boolean | 否    | -      | 在 IoTDB 客户端中启用 rpc 压缩，只在树模型中生效                                                                                                                                                                                                          |
| connection_timeout_in_ms    | Integer | 否    | -      | 连接到 IoTDB 时等待的最长时间（毫秒）                                                                                                                                                                                                                  |
| common-options              |         | 否    | -      | Sink 插件常用参数，详见 [Sink common Options](../Sink common Options.md)                                                                                                                                                                         |


## 示例

### 示例 1： 写入 IoTDB 树模型数据

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

上游 SeaTunnelRow 数据格式如下:

|       device_name        | temperature | moisture |   event_ts    | c_string | c_boolean | c_tinyint | c_smallint | c_int |  c_bigint  | c_float | c_double |
|--------------------------|-------------|----------|---------------|----------|-----------|-----------|------------|-------|------------|---------|----------|
| root.test_group.device_a | 36.1        | 100      | 1664035200001 | abc1     | true      | 1         | 1          | 1     | 2147483648 | 1.0     | 1.0      |
| root.test_group.device_b | 36.2        | 101      | 1664035200001 | abc2     | false     | 2         | 2          | 2     | 2147483649 | 2.0     | 2.0      |
| root.test_group.device_c | 36.3        | 102      | 1664035200001 | abc3     | false     | 3         | 3          | 3     | 2147483649 | 3.0     | 3.0      |

#### 案例 1

只填写所需的配置：
- 使用当前处理时间作为时间戳
- 测点包括排除了`key_device`后的其余字段

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

#### 案例 2

使用源事件的时间：
- 使用指定字段作为时间戳
- 测点包括排除了`key_device`和`key_timestamp`后的其余字段

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

#### 案例 3

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

### 示例 2： 写入 IoTDB 表模型数据

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

上游 SeaTunnelRow 数据格式如下:

| ts                      | model_id | region | tag  | status | arrival_date | temperature |
|-------------------------|----------|--------|------|--------|--------------|-------------|
| 2025-07-30T17:52:34.851 | id1      | 0700HK | tag1 | true   | 2024-11-12   | 4.34        |
| 2025-07-29T17:51:34.851 | id2      | 0700HK | tag2 | false  | 2024-12-01   | 5.54        |
| 2025-07-28T17:50:34.851 | id3      | 0700HK | tag3 | false  | 2024-12-22   | 7.34        |

#### 案例 1

只填写所需的配置:
- 使用当前处理时间作为时间列
- 测量列（FIELD）包括排除了`key_device`后的其余字段

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

IoTDB 数据格式的输出如下:

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

#### 案例 2

使用源事件的时间和限制标签列及属性列：
- 使用指定字段作为时间列
- 使用指定字段作为标签列（TAG）及属性列（ATTRIBUTE）
- 测量列（FIELD）包括排除了`key_device`、`key_timestamp`、`key_tag_fields`和`key_attribute_fields`后的其余字段

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

IoTDB 数据格式的输出如下:

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

#### 案例 3

使用源事件的时间和限制测量列：
- 使用指定字段作为时间列
- 使用指定字段作为测点列（FIELD）

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

IoTDB 数据格式的输出如下:

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

## 变更日志

<ChangeLog />
