import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDB

> IoTDB source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from IoTDB.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
    > IoTDB allows column projection using SQL query.
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Supported DataSource Info

| Datasource | Supported Versions |      Url       |
|------------|--------------------|----------------|
| IoTDB      | `2.0 <= version`   | localhost:6667 |

## Data Type Mapping

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
| STRING          | STRING              |
| TIMESTAMP       | BIGINT              |
| TIMESTAMP       | TIMESTAMP           |
| BLOB            | STRING              |
| DATE            | DATE                |

## Source Options

| Name                       | Type    | Required | Default Value | Description                                                                                                       |
|----------------------------|---------|----------|---------------|-------------------------------------------------------------------------------------------------------------------|
| node_urls                  | Array   | Yes      | -             | IoTDB cluster address, the format is `["host1:port"]` or `["host1:port","host2:port"]`                            |
| username                   | String  | Yes      | -             | IoTDB username                                                                                                    |
| password                   | String  | Yes      | -             | IoTDB user password                                                                                               |
| sql_dialect                | String  | No       | tree          | The sql dialect of IoTDB, options available is `"tree"` or `"table"`                                              |
| database                   | String  | No       | -             | The database selected (only valid when `sql_dielct` is `"table"`)                                                 |
| sql                        | String  | Yes      | -             | The sql statement to be executed                                                                                  |
| schema                     | Config  | Yes      | -             | The data schema                                                                                                   |
| fetch_size                 | Integer | No       | -             | The fetch_size of the IoTDB when you select                                                                       |
| lower_bound                | Long    | No       | -             | The lower_bound of the IoTDB when you select                                                                      |
| upper_bound                | Long    | No       | -             | The upper_bound of the IoTDB when you select                                                                      |
| num_partitions             | Integer | No       | -             | The num_partitions of the IoTDB when you select                                                                   |
| default_thrift_buffer_size | Integer | No       | -             | The thrift_default_buffer_size of the IoTDB when you select                                                       |
| max_thrift_frame_size      | Integer | No       | -             | The thrift max frame size                                                                                         |
| enable_cache_leader        | Boolean | No       | -             | Enable_cache_leader of the IoTDB when you select                                                                  |
| common-options             |         | no       | -             | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details |

We can use time column as a partition key in SQL queries.

#### num_partitions [int]

the number of partitions

### upper_bound [long]

the upper bound of the time range

### lower_bound [long]

the lower bound of the time range

```
     split the time range into numPartitions parts
     if numPartitions = 1, the whole time range will be used
     if numPartitions < (upper_bound - lower_bound), will use (upper_bound - lower_bound) as numPartitions
     
     eg: lower_bound = 1, upper_bound = 10, numPartitions = 2
     sql = "select * from test where age > 0 and age < 10"
     
     split result:
     split 1: select * from test  where (time >= 1 and time < 6)  and (  age > 0 and age < 10 )
     split 2: select * from test  where (time >= 6 and time < 11) and (  age > 0 and age < 10 )
```

## Examples

### Example 1: Read data from IoTDB-tree

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  IoTDB {
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

The data format from upstream IoTDB is as follows:

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

The data format loaded to SeaTunnelRow is as follows:

|      ts       |       device_name        | temperature | moisture | c_int |  c_bigint   | c_float | c_double | c_string | c_boolean |
|---------------|--------------------------|-------------|----------|-------|-------------|---------|----------|----------|-----------|
| 1664035200001 | root.test_group.device_a | 36.1        | 100      | 1     | 21474836470 | 1.0f    | 1.0d     | abc      | true      |
| 1664035200001 | root.test_group.device_b | 36.2        | 101      | 2     | 21474836470 | 2.0f    | 2.0d     | abc      | true      |
| 1664035200001 | root.test_group.device_c | 36.3        | 102      | 3     | 21474836470 | 3.0f    | 3.0d     | abc      | true      |

### Example 2：Read data from IoTDB-table

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  IoTDB {
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

> If database is specified in SQL query, the `database` option is not required.

The data format from upstream IoTDB is as follows:

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

The data format loaded to SeaTunnelRow is as follows:

| ts                      | sn     | type | bidprice | bidsize            | domain | buyno | askprice    |
|-------------------------|--------|------|----------|--------------------|--------|-------|-------------|
| 2025-07-30T17:52:34.851 | 0700HK | L1   | 9        | 10.323907796459721 | true   | 10    | -1064754527 |
| 2025-07-30T17:52:34.951 | 0700HK | L1   | 10       | 9.844574317657585  | false  | 9     | -1088662576 |
| 2025-07-30T17:52:35.051 | 0700HK | L1   | 9        | 9.272974132434069  | true   | 9     | 402003616   |


## Changelog

<ChangeLog />

