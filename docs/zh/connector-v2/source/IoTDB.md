import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDB

> IoTDB 数据读取器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 IoTDB 中读取数据。

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
  > IoTDB 通过 SQL 查询支持列投影功能。
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

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
| TIMESTAMP  | BIGINT         |
| TIMESTAMP  | TIMESTAMP      |
| BLOB       | STRING         |
| DATE       | DATE           |

## Source 选项

| 名称                         | 类型      | 是否必填 | 默认值 | 描述                                                                               |
|----------------------------|---------|------|-----|----------------------------------------------------------------------------------|
| node_urls                  | string  | 是    | -   | IoTDB 集群地址，格式为 `"host1:port"` 或 `"host1:port,host2:port"`                        |
| username                   | string  | 是    | -   | IoTDB 用户名                                                                        |
| password                   | string  | 是    | -   | IoTDB 用户密码                                                                       |
| sql                        | string  | 是    | -   | 要执行的 SQL 查询语句                                                                    |
| schema                     | config  | 是    | -   | 数据模式定义                                                                           |
| fetch_size                 | int     | 否    | -   | 单次获取数据量：查询时每次从 IoTDB 获取的数据量                                                      |
| lower_bound                | long    | 否    | -   | 时间范围下界（通过时间列进行数据分片时使用）                                                           |
| upper_bound                | long    | 否    | -   | 时间范围上界（通过时间列进行数据分片时使用）                                                           |
| num_partitions             | int     | 否    | -   | 分区数量（通过时间列进行数据分片时使用）：<br/> - 1 个分区：使用完整时间范围 <br/> - 若分区数 < (上界 -下界)，则使用差值作为实际分区数 |
| thrift_default_buffer_size | int     | 否    | -   | Thrift 协议缓冲区大小                                                                   |
| thrift_max_frame_size      | int     | 否    | -   | Thrift 最大帧尺寸                                                                     |
| enable_cache_leader        | boolean | 否    | -   | 是否启用 Leader 节点缓存                                                                 |
| version                    | string  | 否    | -   | 客户端 SQL 语义版本（`V_0_12` / `V_0_13`）                                                |
| common-options             |         | 否    | -   | Source 插件常用参数，详见 [Source common Options](../Source common Options.md)            |

我们可以使用时间列进行分区查询。

### num_partitions [int]

分区数量

### upper_bound [long]

时间范围上界

### lower_bound [long]

时间范围下界

```
     将时间范围分割成 numPartitions 个分区
     
     若 numPartitions = 1，使用完整的时间范围
     若 numPartitions < (upper_bound - lower_bound)，使用 (upper_bound - lower_bound) 个分区
     
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

## 变更日志

<ChangeLog />

