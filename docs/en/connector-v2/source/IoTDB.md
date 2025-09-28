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

| Datasource | Supported Versions           |      Url       |
|------------|------------------------------|----------------|
| IoTDB      | `0.13.0 <= version <= 1.3.X` | localhost:6667 |

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

## Source Options

| Name                       | Type    | Required | Default Value | Description                                                                                                       |
|----------------------------|---------|----------|---------------|-------------------------------------------------------------------------------------------------------------------|
| node_urls                  | string  | yes      | -             | IoTDB cluster address, the format is `"host1:port"` or `"host1:port,host2:port"`                                  |
| username                   | string  | yes      | -             | IoTDB user username                                                                                               |
| password                   | string  | yes      | -             | IoTDB user password                                                                                               |
| sql                        | string  | yes      | -             | execute sql statement                                                                                             |
| schema                     | config  | yes      | -             | the data schema                                                                                                   |
| fetch_size                 | int     | no       | -             | the fetch_size of the IoTDB when you select                                                                       |
| lower_bound                | long    | no       | -             | the lower_bound of the IoTDB when you select                                                                      |
| upper_bound                | long    | no       | -             | the upper_bound of the IoTDB when you select                                                                      |
| num_partitions             | int     | no       | -             | the num_partitions of the IoTDB when you select                                                                   |
| thrift_default_buffer_size | int     | no       | -             | the thrift_default_buffer_size of the IoTDB when you select                                                       |
| thrift_max_frame_size      | int     | no       | -             | the thrift max frame size                                                                                         |
| enable_cache_leader        | boolean | no       | -             | enable_cache_leader of the IoTDB when you select                                                                  |
| version                    | string  | no       | -             | SQL semantic version used by the client, The possible values are: `V_0_12`, `V_0_13`                              |
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

## Changelog

<ChangeLog />

