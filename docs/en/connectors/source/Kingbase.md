import ChangeLog from '../changelog/connector-jdbc.md';

# Kingbase

> JDBC Kingbase Source Connector

## Support Connector Version

- 8.6

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

Read external data source data through JDBC.

## Supported DataSource Info

| Datasource | Supported versions |        Driver        |                   Url                    |                                             Maven                                              |
|------------|--------------------|----------------------|------------------------------------------|------------------------------------------------------------------------------------------------|
| Kingbase   | 8.6                | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [Download](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp kingbase8-8.6.0.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|            Kingbase Data type             |                                                                SeaTunnel Data type                                                                |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL                                      | BOOLEAN                                                                                                                                           |
| INT2                                      | SHORT                                                                                                                                             |
| SMALLSERIAL <br/>SERIAL <br/>INT4         | INT                                                                                                                                               |
| INT8 <br/>BIGSERIAL                       | BIGINT                                                                                                                                            |
| FLOAT4                                    | FLOAT                                                                                                                                             |
| FLOAT8                                    | DOUBLE                                                                                                                                            |
| NUMERIC                                   | DECIMAL((Get the designated column's specified column size),<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT | STRING                                                                                                                                            |
| TIMESTAMP                                 | LOCALDATETIME                                                                                                                                     |
| TIME                                      | LOCALTIME                                                                                                                                         |
| DATE                                      | LOCALDATE                                                                                                                                         |
| Other data type                           | Not supported yet                                                                                                                                 |

## Source Options

|             Name             |    Type    | Required |     Default     |                                                                                                                              Description                                                                                                                              |
|------------------------------|------------|----------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:kingbase8://localhost:54321/test                                                                                                                                                                                |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source, should be `com.kingbase8.Driver`.                                                                                                                                                                      |
| username                         | String     | No       | -               | Connection instance user name                                                                                                                                                                                                                                         |
| password                     | String     | No       | -               | Connection instance password                                                                                                                                                                                                                                          |
| query                        | String     | Yes      | -               | Query statement                                                                                                                                                                                                                                                       |
| connection_check_timeout_sec | Int        | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                    |
| partition_column             | String     | No       | -               | The column name for parallelism's partition, only support numeric type column and string type column.                                                                                                                                                                 |
| partition_lower_bound        | BigDecimal | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                      |
| partition_upper_bound        | BigDecimal | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                      |
| partition_num                | Int        | No       | job parallelism | The number of partition count, only support positive integer. Default value is job parallelism.                                                                                                                                                                       |
| fetch_size                   | Int        | No       | 0               | For queries that return a large number of objects, you can configure <br/> the row fetch size used in the query to improve performance by <br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| use_regex                    | Boolean    | No       | false           | Control regular expression matching for table_path. When set to `true`, the table_path will be treated as a regular expression pattern. When set to `false` or not specified, the table_path will be treated as an exact path (no regex matching).                 |
| table_path                                 | String     | No       | -               | The path to the full path of table, you can use this configuration instead of `query`. <br/>example: <br/>"test_schema.table1"                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| table_list                                 | Array      | No       | -               | The list of tables to be read, you can use this configuration instead of `table_path` example: ```[{ table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select * id, name from testdb.table2"}]```                                                                                                                                                                                                                                                                                                                                                                                               |
| where_condition                            | String     | No       | -               | Common row filter conditions for all tables/queries, must start with `where`. for example `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| split.size                                 | Int        | No       | 8096            | The split size (number of rows) of table, captured tables are split into multiple splits when read of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.even-distribution.factor.lower-bound | Double     | No       | 0.05            | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| split.even-distribution.factor.upper-bound | Double     | No       | 100             | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| split.sample-sharding.threshold            | Int        | No       | 10000           | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| split.inverse-sampling.rate                | Int        | No       | 1000            | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| common-options               |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details                                                                                                                                                     |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple

```
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
  }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transforms/sql
}

sink {
    Console {}
}
```

### Parallel

> Read your query table in parallel with the shard field you configured and the shard data. You can do this if you want to read the whole table

```
source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
    # Parallel sharding reads fields
    partition_column = "id"
    # Number of fragments
    partition_num = 10
  }
}
```

### Parallel Boundary

> It is more efficient to read your data source according to the upper and lower boundaries you configured

```
source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
    partition_column = "id"
    partition_num = 10
    # Read start boundary
    partition_lower_bound = 1
    # Read end boundary
    partition_upper_bound = 500
  }
}
```

## Changelog

<ChangeLog />