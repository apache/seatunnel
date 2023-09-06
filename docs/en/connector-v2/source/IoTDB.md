# IoTDB

> IoTDB source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Read external data source data through IoTDB.

:::tip

There is a conflict of thrift version between IoTDB and Spark.Therefore, you need to execute `rm -f $SPARK_HOME/jars/libthrift*` and `cp $IOTDB_HOME/lib/libthrift* $SPARK_HOME/jars/` to resolve it.

:::

## Supported DataSource Info

| Datasource | Supported Versions |      Url       |
|------------|--------------------|----------------|
| IoTDB      | `>= 0.13.0`        | localhost:6667 |

## Data Type Mapping

| IotDB Data type | SeaTunnel Data type |
|-----------------|---------------------|
| BOOLEAN         | BOOLEAN             |
| INT32           | TINYINT             |
| INT32           | SMALLINT            |
| INT32           | INT                 |
| INT64           | BIGINT              |
| FLOAT           | FLOAT               |
| DOUBLE          | DOUBLE              |
| TEXT            | STRING              |

## Source Options

|            Name            |  Type   | Required | Default Value |                                    Description                                     |
|----------------------------|---------|----------|---------------|------------------------------------------------------------------------------------|
| node_urls                  | string  | yes      | -             | `IoTDB` cluster address, the format is `"host1:port"` or `"host1:port,host2:port"` |
| username                   | string  | yes      | -             | `IoTDB` user username                                                              |
| password                   | string  | yes      | -             | `IoTDB` user password                                                              |
| sql                        | string  | yes      | -             | execute sql statement                                                              |
| schema                     | config  | yes      | -             | the data schema                                                                    |
| fetch_size                 | int     | no       | -             | the fetch_size of the IoTDB when you select                                        |
| lower_bound                | long    | no       | -             | the lower_bound of the IoTDB when you select                                       |
| upper_bound                | long    | no       | -             | the upper_bound of the IoTDB when you select                                       |
| num_partitions             | int     | no       | -             | the num_partitions of the IoTDB when you select                                    |
| thrift_default_buffer_size | int     | no       | -             | the thrift_default_buffer_size of the IoTDB when you select                        |
| thrift_max_frame_size      | int     | no       | -             | the thrift max frame size                                                          |
| enable_cache_leader        | boolean | no       | -             | enable_cache_leader of the IoTDB when you select                                   |
| version                    | string  | no       | -             | SQL semantic version used by the client, The possible values are: V_0_12, V_0_13   |
| common-options             |         | no       | -             |                                                                                    |

### split partitions

we can split the partitions of the IoTDB and we used time column split

#### num_partitions [int]

split num

### upper_bound [long]

upper bound of the time column

### lower_bound [long]

lower bound of the time column

```
     split the time range into numPartitions parts
     if numPartitions is 1, use the whole time range
     if numPartitions < (upper_bound - lower_bound), use (upper_bound - lower_bound) partitions
     
     eg: lower_bound = 1, upper_bound = 10, numPartitions = 2
     sql = "select * from test where age > 0 and age < 10"
     
     split result

     split 1: select * from test  where (time >= 1 and time < 6)  and (  age > 0 and age < 10 )
     
     split 2: select * from test  where (time >= 6 and time < 11) and (  age > 0 and age < 10 )

```

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Examples

```hocon
env {
  execution.parallelism = 2
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

Upstream `IoTDB` data format is the following:

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

Loaded to SeaTunnelRow data format is the following:

|      ts       |       device_name        | temperature | moisture | c_int |  c_bigint   | c_float | c_double | c_string | c_boolean |
|---------------|--------------------------|-------------|----------|-------|-------------|---------|----------|----------|-----------|
| 1664035200001 | root.test_group.device_a | 36.1        | 100      | 1     | 21474836470 | 1.0f    | 1.0d     | abc      | true      |
| 1664035200001 | root.test_group.device_b | 36.2        | 101      | 2     | 21474836470 | 2.0f    | 2.0d     | abc      | true      |
| 1664035200001 | root.test_group.device_c | 36.3        | 102      | 3     | 21474836470 | 3.0f    | 3.0d     | abc      | true      |

## Changelog

### 2.2.0-beta 2022-09-26

- Add IoTDB Source Connector

### 2.3.0-beta 2022-10-20

- [Improve] Improve IoTDB Source Connector ([2917](https://github.com/apache/seatunnel/pull/2917))
  - Support extract timestamp、device、measurement from SeaTunnelRow
  - Support TINYINT、SMALLINT
  - Support flush cache to database before prepareCommit

