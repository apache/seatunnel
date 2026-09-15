import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDBv2

> IoTDBv2 数据接收器

## 描述

用于将数据写入 IoTDB 2.x。作业配置中的连接器名称为 `IoTDBv2`。

连接器同时支持 IoTDB 树模型（`sql_dialect = "tree"`，默认）和表模型（`sql_dialect = "table"`）。每一条上游记录会被写入一条 IoTDB 记录，树模型下是某个设备的时序记录，表模型下是某个表的一行。树模型在 `(device, timestamp)` 上幂等，表模型在表的主键上幂等，这也是它能够提供精确一次保证的基础。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

    > IoTDB 通过幂等写支持`精确一次`功能。如果两条数据使用相同的`key`和`timestamp`，新数据将覆盖旧数据。
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

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
| sql_dialect                 | String  | 否    | tree   | IoTDB 模型，可选值为 `tree` 和 `table`。`tree` 表示树模型，`table` 表示表模型。                                                                                                                                                                      |
| storage_group               | String  | 是    | -      | IoTDB 树模型：指定设备路径前缀。例如设备路径为 `storage_group + "." + key_device`。如果 `key_device` 已经是完整设备路径，可以设置为空字符串。<br/> IoTDB 表模型：指定数据库。                                                                                           |
| key_device                  | String  | 是    | -      | IoTDB 树模型：在 SeaTunnelRow 中指定 IoTDB 设备 ID 的字段名；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 表名的字段名                                                                                                                                           |
| key_timestamp               | String  | 否    | 数据处理时间 | IoTDB 树模型：在 SeaTunnelRow 中指定 IoTDB 时间戳的字段名（如未指定，则使用处理时间作为时间戳）；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 时间列的字段名（如未指定，则使用处理时间作为时间戳）                                                                                                       |
| key_measurement_fields      | Array   | 否    | 见描述    | IoTDB 树模型：在 SeaTunnelRow 中指定 IoTDB 测量列表的字段名（如未指定，则包括排除`key_device`&`key_timestamp`后的其余字段）；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 测点列（FIELD）的字段名（如未指定，则包括排除`key_device`&`key_timestamp`&`key_tag_fields`&`key_attribute_fields`后的其余字段） |
| key_tag_fields              | Array   | 否    | -      | IoTDB 树模型：不生效；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 标签列（TAG）的字段名                                                                                                                                                                     |
| key_attribute_fields        | Array   | 否    | -      | IoTDB 树模型：不生效；<br/> IoTDB 表模型：在 SeaTunnelRow 中指定 IoTDB 属性列（ATTRIBUTE）的字段名                                                                                                                                                               |
| batch_size                  | Integer | 否    | 1024   | 缓存的记录数达到 `batch_size` 时，连接器会把数据刷新到 IoTDB；在 checkpoint 提交前和写入器关闭时也会刷新。                                                                                                                                                                  |
| max_retries                 | Integer | 否    | -      | 刷新失败时的最大重试次数。如果不填写，刷新失败时不会自动重试。                                                                                                                                                                                                  |
| retry_backoff_multiplier_ms | Integer | 否    | -      | 计算重试等待时间的退避倍数，单位为毫秒。和 `max_retry_backoff_ms` 都不填写时不使用指数退避。                                                                                                                                                                              |
| max_retry_backoff_ms        | Integer | 否    | -      | 最大重试等待时间，单位为毫秒。                                                                                                                                                                                                                         |
| default_thrift_buffer_size  | Integer | 否    | -      | IoTDB 客户端使用的默认 Thrift 缓冲区大小。                                                                                                                                                                                                             |
| max_thrift_frame_size       | Integer | 否    | -      | IoTDB 客户端使用的最大 Thrift 帧大小。                                                                                                                                                                                                               |
| zone_id                     | String  | 否    | -      | IoTDB 客户端使用的 `java.time.ZoneId`。                                                                                                                                                                                                        |
| enable_rpc_compression      | Boolean | 否    | -      | 在 IoTDB 客户端中启用 rpc 压缩，只在树模型中生效                                                                                                                                                                                                          |
| connection_timeout_in_ms    | Integer | 否    | -      | 连接到 IoTDB 时等待的最长时间（毫秒）                                                                                                                                                                                                                  |
| common-options              |         | 否    | -      | Sink 插件常用参数，详见 [Sink 常用选项](../common-options/sink-common-options.md)                                                                                                                                                                              |

树模型下，`key_device` 用作 IoTDB 设备路径，`key_measurement_fields` 决定哪些字段写成测点。未配置 `key_measurement_fields` 时，除 `key_device` 和 `key_timestamp` 之外的所有字段都会写成测点。

表模型下，`storage_group` 表示数据库，`key_device` 表示目标表名字段，`key_tag_fields` 表示 TAG 列，`key_attribute_fields` 表示 ATTRIBUTE 列，`key_measurement_fields` 表示 FIELD 列。未配置 `key_measurement_fields` 时，除表名、时间、TAG 和 ATTRIBUTE 字段之外的所有字段都会写成 FIELD 列。

连接器会缓存数据行，当缓存大小达到 `batch_size` 时刷新到 IoTDB。checkpoint 提交前和写入器关闭时也会触发刷新。连接器不提供独立的定时刷新配置，请通过 `batch_size` 在吞吐和时延之间取舍。

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
  IoTDBv2 {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    storage_group = "root.test_group"
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
|2023-09-01T00:00:00.001Z|root.test_group.device_c|          36.3|        102| 1664035200001|     abc3|     false|         3|          3|     3| 2147483649|     3.0|      3.0|
+------------------------+------------------------+--------------+-----------+--------------+---------+---------+-----------+-----------+------+-----------+--------+---------+
```

#### 案例 2

使用源事件的时间：
- 使用指定字段作为时间戳
- 测点包括排除了`key_device`和`key_timestamp`后的其余字段

```hocon
sink {
  IoTDBv2 {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    storage_group = "root.test_group"
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
|2022-09-25T00:00:00.001Z|root.test_group.device_c|          36.3|        102| 1664035200001|     abc3|     false|         3|          3|     3| 2147483649|     3.0|      3.0|
+------------------------+------------------------+--------------+-----------+--------------+---------+---------+-----------+-----------+------+-----------+--------+---------+
```

#### 案例 3

使用源事件的时间和限制测量字段：
- 使用指定字段作为时间戳
- 测点仅包括`key_measurement_fields`指定的字段

```hocon
sink {
  IoTDBv2 {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    storage_group = "root.test_group"
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
  IoTDBv2 {
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
  IoTDBv2 {
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
  IoTDBv2 {
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