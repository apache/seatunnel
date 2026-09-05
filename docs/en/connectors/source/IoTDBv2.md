import ChangeLog from '../changelog/connector-iotdb.md';

# IoTDBv2

> IoTDBv2 source connector

## Description

Used to read data from IoTDB 2.x. The connector name in job configuration is `IoTDBv2`.

The connector supports both the IoTDB tree model (`sql_dialect = "tree"`, default) and the table model (`sql_dialect = "table"`). It executes the configured SQL query against IoTDB and produces rows whose schema is defined by the `schema` option.

For the tree model the connector relies on the IoTDB `align by device` clause (the example queries below use it). For the table model, queries are plain `SELECT` statements against an IoTDB table; if the SQL itself names the database, `database` does not need to be configured.

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
    > IoTDB allows column projection using SQL query.
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource | Supported Versions |      Url       |
|------------|--------------------|----------------|
| IoTDB      | `2.0 <= version`   | localhost:6667 |

## Data Type Mapping

| IoTDB Data Type | SeaTunnel Data Type |
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
| sql_dialect                | String  | No       | tree          | The SQL dialect of IoTDB. Available values are `"tree"` and `"table"`.                                            |
| database                   | String  | No       | -             | The selected database. This option is only valid when `sql_dialect` is `"table"`.                                 |
| sql                        | String  | Yes      | -             | The sql statement to be executed                                                                                  |
| schema                     | Config  | Yes      | -             | The data schema. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).|
| fetch_size                 | Integer | No       | -             | The number of rows fetched from IoTDB in one request.                                                             |
| lower_bound                | Long    | No       | -             | The lower bound of the time range used for source partition splitting.                                            |
| upper_bound                | Long    | No       | -             | The upper bound of the time range used for source partition splitting.                                            |
| num_partitions             | Integer | No       | -             | The number of partitions used to split the time range.                                                            |
| default_thrift_buffer_size | Integer | No       | -             | The default Thrift buffer size used by the IoTDB client.                                                          |
| max_thrift_frame_size      | Integer | No       | -             | The thrift max frame size                                                                                         |
| enable_cache_leader        | Boolean | No       | -             | Whether to enable leader cache in the IoTDB client.                                                               |
| common-options             |         | No       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details |

You can use the time column to split a source query into multiple partitions. To enable this, set `lower_bound`, `upper_bound`, and `num_partitions` together.

### node_urls [Array]

The IoTDB cluster endpoints. Each entry must be in the form `host:port`, for example `["iotdb-1:6667"]` or `["iotdb-1:6667","iotdb-2:6667"]`.

### username [String]

The IoTDB username used to authenticate against the cluster.

### password [String]

The IoTDB password used to authenticate against the cluster.

### sql_dialect [String]

The IoTDB model the connector talks to. The default value is `"tree"`, which uses the IoTDB tree model and expects queries like `SELECT ... FROM root.x.y align by device`. Set it to `"table"` to query the IoTDB table model with plain `SELECT` statements.

### database [String]

The IoTDB database (catalog) used by table-model queries. Only valid when `sql_dialect = "table"`. When the SQL statement itself fully qualifies the database, this option is not required.

### sql [String]

The SQL query to execute against IoTDB.

For the tree model, the query must include `align by device` so the connector can read each device's measurements as separate rows. For the table model, plain `SELECT ... FROM <table>` queries are used.

### schema [Config]

Defines the SeaTunnel row type that the connector should produce. Each field in the schema maps to one column returned by the query. See [Schema Feature](../../introduction/concepts/schema-feature.md) for the full schema syntax.

### fetch_size [Integer]

The number of rows fetched from IoTDB in a single request. When unset, the IoTDB client default is used.

### lower_bound [Long]

The lower bound (inclusive) of the time range used for source partition splitting, expressed as a millisecond timestamp. Must be used together with `upper_bound` and `num_partitions`.

### upper_bound [Long]

The upper bound (exclusive) of the time range used for source partition splitting, expressed as a millisecond timestamp. Must be used together with `lower_bound` and `num_partitions`.

### num_partitions [Integer]

The number of partitions the source splits the time range into. Must be used together with `lower_bound` and `upper_bound`.

The split behavior is:

- When `num_partitions = 1`, the entire time range is used as a single partition.
- When `num_partitions < (upper_bound - lower_bound)`, the connector uses `(upper_bound - lower_bound)` as the actual number of partitions.

For example, with `lower_bound = 1`, `upper_bound = 10`, `num_partitions = 2`, and `sql = "select * from test where age > 0 and age < 10"`, the connector rewrites the SQL into:

```sql
split 1: select * from test where (time >= 1  and time < 6)  and (age > 0 and age < 10)
split 2: select * from test where (time >= 6  and time < 11) and (age > 0 and age < 10)
```

### default_thrift_buffer_size [Integer]

The default Thrift buffer size used by the IoTDB client. Leave unset to use the client default.

### max_thrift_frame_size [Integer]

The maximum Thrift frame size used by the IoTDB client. Increase this when reading very large rows. Leave unset to use the client default.

### enable_cache_leader [Boolean]

Whether to enable leader caching in the IoTDB client. When unset, the client default is used.

### common-options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Examples

### Example 1: Read data from IoTDB-tree

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
| 1664035200001 | root.test_group.device_c | 36.3        | 102      | 3     | 21474836470 | 3.0f    | 1.0d     | abc      | true      |

### Example 2: Read data from IoTDB-table

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
| 2025-07-30T17:52:34.851 | 0700HK | L1   | 9        | 10.323907796459721 | true | 10    | -1064754527 |
| 2025-07-30T17:52:34.951 | 0700HK | L1   | 10       | 9.844574317657585  | false | 9    | -1088662576 |
| 2025-07-30T17:52:35.051 | 0700HK | L1   | 9        | 9.272974132434069  | true | 9     | 402003616   |

## Changelog

<ChangeLog />