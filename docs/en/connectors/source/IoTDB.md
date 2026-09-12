import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDB

> IoTDB source connector
 
## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from IoTDB.

The current source runs a bounded SQL query. It is suitable for batch reads and does not continuously tail new IoTDB data in streaming mode.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
  > IoTDB allows column projection using SQL query.
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

> The IoTDB source connector issues a single bounded SQL query per task. It is intended for batch jobs or bounded time-window reads. It does not maintain a streaming cursor over the IoTDB change log, so it cannot tail new writes in streaming mode.

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
| STRING          | STRING              |
| time column     | BIGINT              |
| time column     | TIMESTAMP           |

## Source Options

| Name                       | Type    | Required | Default Value | Description                                                                                                       |
|----------------------------|---------|----------|---------------|-------------------------------------------------------------------------------------------------------------------|
| node_urls                  | string  | yes      | -             | IoTDB cluster address, the format is `"host1:port"` or `"host1:port,host2:port"`                                  |
| username                   | string  | yes      | -             | IoTDB user username                                                                                               |
| password                   | string  | yes      | -             | IoTDB user password                                                                                               |
| sql                        | string  | yes      | -             | execute sql statement                                                                                             |
| schema                     | config  | yes      | -             | The data schema. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).                                                                                                   |
| fetch_size                 | int     | no       | -             | Number of rows fetched from IoTDB in one request.                                                                 |
| lower_bound                | long    | no       | -             | Lower time bound used when SeaTunnel splits the query by time.                                                     |
| upper_bound                | long    | no       | -             | Upper time bound used when SeaTunnel splits the query by time.                                                     |
| num_partitions             | int     | no       | -             | Number of time-range partitions. Use it together with `lower_bound` and `upper_bound`.                             |
| thrift_default_buffer_size | int     | no       | -             | Initial Thrift buffer size for the IoTDB client.                                                                  |
| thrift_max_frame_size      | int     | no       | -             | Maximum Thrift frame size for the IoTDB client.                                                                   |
| enable_cache_leader        | boolean | no       | -             | Whether to cache the leader node in the IoTDB client.                                                             |
| version                    | string  | no       | -             | SQL semantic version used by the client. The possible values are `V_0_12` and `V_0_13`.                           |
| common-options             |         | no       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details |

The first field in `schema.fields` must describe the IoTDB time column. It can be `bigint` when you want epoch milliseconds, or `timestamp` when you want a SeaTunnel timestamp value.

When the SQL uses `align by device`, the second field normally describes the IoTDB device name. The remaining fields must follow the same order as the measurements returned by the SQL query.

You can use the time column as a partition key in SQL queries.

#### num_partitions [int]

the number of partitions

### upper_bound [long]

the upper bound of the time range

### lower_bound [long]

the lower bound of the time range

```
     split the time range into numPartitions parts
     if numPartitions = 1, the whole time range will be used
     if (upper_bound - lower_bound) < numPartitions, will use (upper_bound - lower_bound) as numPartitions
     
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

`lower_bound`, `upper_bound`, and `num_partitions` are optional. They are useful when the query covers a large time range and you want SeaTunnel to split the read into multiple time partitions.

The following example reads from one IoTDB path, replaces the device prefix, and writes the result to another IoTDB path.

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

## Time-Range Partitioning

`lower_bound`, `upper_bound`, and `num_partitions` together let SeaTunnel split a single bounded query into multiple sub-queries, one per time partition. The partitions are distributed across the configured `parallelism`, so each sub-task owns a non-overlapping time slice.

The split rule is:

- If `num_partitions = 1`, the entire range `[lower_bound, upper_bound)` is used as a single partition.
- Otherwise the range is divided into `num_partitions` equal slices. If `upper_bound - lower_bound < num_partitions`, the connector falls back to `upper_bound - lower_bound` partitions.

For example, with `lower_bound = 1`, `upper_bound = 10`, and `num_partitions = 2` against the SQL `select * from test where age > 0 and age < 10`, the connector generates:

```sql
-- split 1
select * from test where (time >= 1 and time < 6)  and (age > 0 and age < 10);
-- split 2
select * from test where (time >= 6 and time < 11) and (age > 0 and age < 10);
```

Use this when the time column is the natural partition key and the underlying data set spans a wide time range. For non-time-bound parallelism, raise `parallelism` instead and rely on IoTDB-side partitioning.

## Changelog

<ChangeLog />
