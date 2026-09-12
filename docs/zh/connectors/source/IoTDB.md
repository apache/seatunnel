import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDB

> IoTDB 数据读取器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 IoTDB 中读取数据。

当前 Source 执行的是有界 SQL 查询，适合批量读取；即使任务使用流模式，也不会持续监听 IoTDB 新数据。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
  > IoTDB 通过 SQL 查询支持列投影功能。
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

> IoTDB Source 在每个子任务上执行一次有界 SQL 查询，适合批处理作业或带时间窗口的边界读取。它不会持续订阅 IoTDB 的变更日志，因此无法在流式模式下持续拉取新增数据。

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
| STRING     | STRING         |
| 时间列        | BIGINT         |
| 时间列        | TIMESTAMP      |

## Source 选项

| 名称                         | 类型      | 是否必填 | 默认值 | 描述                                                                               |
|----------------------------|---------|------|-----|----------------------------------------------------------------------------------|
| node_urls                  | string  | 是    | -   | IoTDB 集群地址，格式为 `"host1:port"` 或 `"host1:port,host2:port"`                        |
| username                   | string  | 是    | -   | IoTDB 用户名                                                                        |
| password                   | string  | 是    | -   | IoTDB 用户密码                                                                       |
| sql                        | string  | 是    | -   | 要执行的 SQL 查询语句                                                                    |
| schema                     | config  | 是    | -   | 数据模式定义。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                                                           |
| fetch_size                 | int     | 否    | -   | 单次获取数据量：查询时每次从 IoTDB 获取的数据量                                                      |
| lower_bound                | long    | 否    | -   | 时间范围下界（通过时间列进行数据分片时使用）                                                           |
| upper_bound                | long    | 否    | -   | 时间范围上界（通过时间列进行数据分片时使用）                                                           |
| num_partitions             | int     | 否    | -   | 分区数量（通过时间列进行数据分片时使用）：<br/> - 1 个分区：使用完整时间范围 <br/> - 若分区数 < (上界 -下界)，则使用差值作为实际分区数 |
| thrift_default_buffer_size | int     | 否    | -   | Thrift 协议缓冲区大小                                                                   |
| thrift_max_frame_size      | int     | 否    | -   | Thrift 最大帧尺寸                                                                     |
| enable_cache_leader        | boolean | 否    | -   | 是否启用 Leader 节点缓存                                                                 |
| version                    | string  | 否    | -   | 客户端 SQL 语义版本（`V_0_12` / `V_0_13`）                                                |
| common-options             |         | 否    | -   | Source 插件通用参数，详见 [Source 通用选项](../common-options/source-common-options.md)            |

`schema.fields` 中的第一个字段必须对应 IoTDB 时间列。需要毫秒时间戳时可以配置为 `bigint`，需要 SeaTunnel timestamp 值时可以配置为 `timestamp`。

当 SQL 使用 `align by device` 时，第二个字段通常对应 IoTDB 设备名。后续字段需要和 SQL 返回的测点顺序保持一致。

可以使用时间列进行分区查询。

### num_partitions [int]

分区数量

### upper_bound [long]

时间范围上界

### lower_bound [long]

时间范围下界

```
     将时间范围分割成 numPartitions 个分区
     
     若 numPartitions = 1，使用完整的时间范围
     若 (upper_bound - lower_bound) < numPartitions，使用 (upper_bound - lower_bound) 个分区
     
     例：lower_bound = 1, upper_bound = 10, numPartitions = 2
         sql = "select * from test where age > 0 and age < 10"
     
     分区结果：
     split 1: select * from test  where (time >= 1 and time < 6)  and (  age > 0 and age < 10 )
     split 2: select * from test  where (time >= 6 and time < 11) and (  age > 0 and age < 10 )
```


## 示例

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  IoTDB {
    node_urls = "localhost:6667"
    username = "root"
    password = "root"
    sql = "SELECT temperature, moisture, c_int, c_bigint, c_float, c_double, c_string, c_boolean FROM root.test_group.* WHERE time < 4102329600000 align by device"
    lower_bound = 1
    upper_bound = 4102329600000
    num_partitions = 10
    schema {
      fields {
        ts = timestamp
        device_name = string
        temperature = float
        moisture = bigint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_string = string
        c_boolean = boolean
      }
    }
  }
}

sink {
  Console {
  }
}
```

`lower_bound`、`upper_bound` 和 `num_partitions` 是可选项。当查询覆盖很大的时间范围时，可以用它们把读取任务按时间范围拆成多个分区。

下面的示例从一个 IoTDB 路径读取数据，替换设备名前缀后，再写入另一个 IoTDB 路径。

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  IoTDB {
    plugin_output = "iotdb_rows"
    node_urls = "localhost:6667"
    username = "root"
    password = "root"
    sql = "SELECT c_string, c_boolean, c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double FROM root.source_group.* WHERE time < 4102329600000 align by device"
    lower_bound = 1
    upper_bound = 4102329600000
    num_partitions = 10
    schema {
      fields {
        ts = timestamp
        device_name = string
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
      }
    }
  }
}

transform {
  Replace {
    plugin_input = "iotdb_rows"
    plugin_output = "sink_rows"
    replace_field = "device_name"
    pattern = "root.source_group"
    replacement = "root.sink_group"
    is_regex = false
    replace_first = true
  }
}

sink {
  IoTDB {
    plugin_input = "sink_rows"
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    key_device = "device_name"
    key_timestamp = "ts"
    key_measurement_fields = ["c_string", "c_boolean", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_float", "c_double"]
    batch_size = 1
  }
}
```

上游 IoTDB 的数据格式如下所示:

```shell
IoTDB> SELECT temperature, moisture, c_int, c_bigint, c_float, c_double, c_string, c_boolean FROM root.test_group.* WHERE time < 4102329600000 align by device;
+------------------------+------------------------+--------------+-----------+--------+--------------+----------+---------+---------+----------+
|                    Time|                  Device|   temperature|   moisture|   c_int|      c_bigint|   c_float| c_double| c_string| c_boolean|
+------------------------+------------------------+--------------+-----------+--------+--------------+----------+---------+---------+----------+
|2022-09-25T00:00:00.001Z|root.test_group.device_a|          36.1|        100|       1|   21474836470|      1.0f|     1.0d|      abc|      true|
|2022-09-25T00:00:00.001Z|root.test_group.device_b|          36.2|        101|       2|   21474836470|      2.0f|     2.0d|      abc|      true|
|2022-09-25T00:00:00.001Z|root.test_group.device_c|          36.3|        102|       3|   21474836470|      3.0f|     3.0d|      abc|      true|
+------------------------+------------------------+--------------+-----------+--------+--------------+----------+---------+---------+----------+
```

读取到 SeaTunnelRow 的数据格式如下所示:

|      ts       |       device_name        | temperature | moisture | c_int |  c_bigint   | c_float | c_double | c_string | c_boolean |
|---------------|--------------------------|-------------|----------|-------|-------------|---------|----------|----------|-----------|
| 1664035200001 | root.test_group.device_a | 36.1        | 100      | 1     | 21474836470 | 1.0f    | 1.0d     | abc      | true      |
| 1664035200001 | root.test_group.device_b | 36.2        | 101      | 2     | 21474836470 | 2.0f    | 2.0d     | abc      | true      |
| 1664035200001 | root.test_group.device_c | 36.3        | 102      | 3     | 21474836470 | 3.0f    | 3.0d     | abc      | true      |

## 时间范围分片

`lower_bound`、`upper_bound`、`num_partitions` 三个参数联合使用，可以让 SeaTunnel 把一次有界查询拆分成多条按时间分片的子查询，再按 `parallelism` 分配到各子任务，每个子任务负责一个互不重叠的时间段。

拆分规则：

- 如果 `num_partitions = 1`，则整个范围 `[lower_bound, upper_bound)` 作为一个分片。
- 否则把范围等分成 `num_partitions` 个分片。如果 `upper_bound - lower_bound < num_partitions`，则回退为 `upper_bound - lower_bound` 个分片。

例如 `lower_bound = 1`、`upper_bound = 10`、`num_partitions = 2`，SQL 为 `select * from test where age > 0 and age < 10`，会生成：

```sql
-- split 1
select * from test where (time >= 1 and time < 6)  and (age > 0 and age < 10);
-- split 2
select * from test where (time >= 6 and time < 11) and (age > 0 and age < 10);
```

当时间列是天然分区键且底层数据时间跨度很大时，适合使用该机制；如果不是按时间维度并行，请直接调高 `parallelism`，由 IoTDB 端处理并行。

## 变更日志

<ChangeLog />
