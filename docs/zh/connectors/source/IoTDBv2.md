import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDBv2

> IoTDBv2 数据读取器

## 描述

用于从 IoTDB 2.x 中读取数据。作业配置中的连接器名称为 `IoTDBv2`。

连接器同时支持 IoTDB 树模型（`sql_dialect = "tree"`，默认）和表模型（`sql_dialect = "table"`）。它会根据配置的 SQL 语句向 IoTDB 发起查询，并把结果按 `schema` 选项定义的字段结构输出成 SeaTunnel 行。

树模型下查询必须使用 `align by device` 子句（下面示例中即采用这种写法），这样读取器才能把每个设备的测点作为独立的行输出。表模型下使用普通的 `SELECT` 语句；如果 SQL 本身已经写明了数据库名，可以不填写 `database` 选项。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md) 
  > IoTDB 通过 SQL 查询支持列投影功能。
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源   | 支持的版本            | 地址             |
|-------|------------------|----------------|
| IoTDB | `2.0 <= version` | localhost:6667 |

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

| 名称                         | 类型      | 是否必填 | 默认值  | 描述                                                                               |
|----------------------------|---------|------|------|----------------------------------------------------------------------------------|
| node_urls                  | Array   | 是    | -    | IoTDB 集群地址，格式为 `["host1:port"]` 或 `["host1:port","host2:port"]`                  |
| username                   | String  | 是    | -    | IoTDB 用户名                                                                        |
| password                   | String  | 是    | -    | IoTDB 用户密码                                                                       |
| sql_dialect                | String  | 否    | tree | IoTDB 模型，可选值为 `tree` 和 `table`。`tree` 表示树模型，`table` 表示表模型。                         |
| database                   | String  | 否    | -    | 要查询的数据库名，只在表模型中生效                                                                |
| sql                        | String  | 是    | -    | 要执行的 SQL 查询语句                                                                    |
| schema                     | Config  | 是    | -    | 数据模式定义。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                                                           |
| fetch_size                 | Integer | 否    | -    | 单次请求从 IoTDB 获取的行数。                                                               |
| lower_bound                | Long    | 否    | -    | 时间范围下界（通过时间列进行数据分片时使用），单位毫秒                                                          |
| upper_bound                | Long    | 否    | -    | 时间范围上界（通过时间列进行数据分片时使用），单位毫秒                                                          |
| num_partitions             | Integer | 否    | -    | 分区数量（通过时间列进行数据分片时使用）。需要和 `lower_bound`、`upper_bound` 一起配置。                                  |
| default_thrift_buffer_size | Integer | 否    | -    | IoTDB 客户端使用的默认 Thrift 缓冲区大小。                                                     |
| max_thrift_frame_size      | Integer | 否    | -    | Thrift 最大帧尺寸                                                                     |
| enable_cache_leader        | Boolean | 否    | -    | 是否在 IoTDB 客户端启用 Leader 节点缓存。                                                     |
| common-options             |         | 否    | -    | Source 插件常用参数，详见 [Source 常用选项](../common-options/source-common-options.md)                  |

可以使用时间列把一次查询拆成多个分片执行。启用分片读取时，需要同时配置 `lower_bound`、`upper_bound` 和 `num_partitions`。

### node_urls [Array]

IoTDB 集群地址列表，每个元素必须是 `host:port` 形式，例如 `["iotdb-1:6667"]` 或 `["iotdb-1:6667","iotdb-2:6667"]`。

### username [String]

连接 IoTDB 集群所使用的用户名。

### password [String]

连接 IoTDB 集群所使用的用户密码。

### sql_dialect [String]

IoTDB 模型类型，默认值为 `tree`，表示 IoTDB 树模型，查询语句形如 `SELECT ... FROM root.x.y align by device`。如果需要使用 IoTDB 表模型，请设置为 `table`，并使用普通的 `SELECT ... FROM <表名>` 语句。

### database [String]

表模型下要查询的数据库名。仅在 `sql_dialect = "table"` 时生效。如果 SQL 语句本身已经写明了数据库，可以不填写此选项。

### sql [String]

要执行的 SQL 查询语句。

树模型下，查询必须包含 `align by device` 子句，否则读取器无法把每个设备的测点作为独立行输出。表模型下使用普通的 `SELECT` 语句即可。

### schema [Config]

SeaTunnel 行结构定义，描述查询结果中每一列对应的 SeaTunnel 字段类型。更多 schema 写法请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### fetch_size [Integer]

单次请求从 IoTDB 拉取的行数。不填写时使用 IoTDB 客户端的默认值。

### lower_bound [Long]

时间范围下界（包含），单位为毫秒时间戳。需要和 `upper_bound`、`num_partitions` 一起配置。

### upper_bound [Long]

时间范围上界（不包含），单位为毫秒时间戳。需要和 `lower_bound`、`num_partitions` 一起配置。

### num_partitions [Integer]

时间范围被切分的分区数量。需要和 `lower_bound`、`upper_bound` 一起配置。

具体行为：

- 当 `num_partitions = 1` 时，整个时间范围会作为一个分区使用。
- 当 `num_partitions < (upper_bound - lower_bound)` 时，连接器会使用 `(upper_bound - lower_bound)` 作为实际分区数。

例如 `lower_bound = 1`，`upper_bound = 10`，`num_partitions = 2`，`sql = "select * from test where age > 0 and age < 10"`，连接器会改写为：

```sql
split 1: select * from test where (time >= 1  and time < 6)  and (age > 0 and age < 10)
split 2: select * from test where (time >= 6  and time < 11) and (age > 0 and age < 10)
```

### default_thrift_buffer_size [Integer]

IoTDB 客户端使用的默认 Thrift 缓冲区大小。不填写时使用客户端默认值。

### max_thrift_frame_size [Integer]

IoTDB 客户端的最大 Thrift 帧大小，读取较大记录时可适当调大。不填写时使用客户端默认值。

### enable_cache_leader [Boolean]

是否在 IoTDB 客户端启用 Leader 节点缓存。不填写时使用客户端默认值。

### 通用选项

Source 插件通用参数，详见 [Source 通用选项](../common-options/source-common-options.md)。

## 示例

### 示例 1：读取 IoTDB 树模型数据

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  IoTDBv2 {
    node_urls = ["localhost:6667"]
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
| 1664035200001 | root.test_group.device_c | 36.3        | 102      | 3     | 21474836470 | 3.0f    | 1.0d     | abc      | true      |

### 示例 2：读取 IoTDB 表模型数据

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  IoTDBv2 {
    node_urls = ["localhost:6667"]
    username = "root"
    password = "root"
    sql_dialect = "table"
    database = "test_database"
    sql = "SELECT time, sn, type, bidprice, bidsize, domain, buyno, askprice FROM test_table"
    schema {
      fields {
        ts = timestamp
        sn = string
        type = string
        bidprice = int
        bidsize = double
        domain = boolean
        buyno = bigint
        askprice = string
      }
    }
  }
}

sink {
  Console {
  }
}
```

> 若查询语句中明确了数据库，则无需使用 `database` 参数

上游 IoTDB 的数据格式如下所示：

```shell
IoTDB> SELECT time, sn, type, bidprice, bidsize, domain, buyno, askprice FROM test_table
+-----------------------------+------+----+--------+------------------+------+-----+-----------+
|                         time|    sn|type|bidprice|           bidsize|domain|buyno|   askprice|
+-----------------------------+------+----+--------+------------------+------+-----+-----------+
|2025-07-30T17:52:34.851+08:00|0700HK|  L1|       9|10.323907796459721|  true|   10|-1064754527|
|2025-07-30T17:52:34.951+08:00|0700HK|  L1|      10| 9.844574317657585| false|    9|-1088662576|
|2025-07-30T17:52:35.051+08:00|0700HK|  L1|       9| 9.272974132434069|  true|    9|  402003616|
+-----------------------------+------+----+--------+------------------+------+-----+-----------+
```

读取到 SeaTunnelRow 的数据格式如下所示：

| ts                      | sn     | type | bidprice | bidsize            | domain | buyno | askprice    |
|-------------------------|--------|------|----------|--------------------|--------|-------|-------------|
| 2025-07-30T17:52:34.851 | 0700HK | L1   | 9        | 10.323907796459721 | true   | 10    | -1064754527 |
| 2025-07-30T17:52:34.951 | 0700HK | L1   | 10       | 9.844574317657585  | false  | 9     | -1088662576 |
| 2025-07-30T17:52:35.051 | 0700HK | L1   | 9        | 9.272974132434069  | true   | 9     | 402003616   |

## 变更日志

<ChangeLog />