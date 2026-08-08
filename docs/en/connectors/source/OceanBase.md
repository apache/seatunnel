import ChangeLog from '../changelog/connector-jdbc.md';

# OceanBase

> JDBC OceanBase Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Description

Read OceanBase data through JDBC. OceanBase can run in MySQL-compatible mode
or Oracle-compatible mode, so every OceanBase job should set `compatible_mode`
to `mysql` or `oracle`.

:::tip

The connector uses the `Jdbc` plugin under the hood with the
`com.oceanbase.jdbc.Driver` driver. The `compatible_mode` option is required
to switch between the MySQL and Oracle dialect implementations.

:::

## Supported DataSource Info

| Datasource | Supported versions       | Driver                   | Url                                  | Maven                                                                       |
|------------|--------------------------|--------------------------|--------------------------------------|-----------------------------------------------------------------------------|
| OceanBase  | All OceanBase server versions. | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [Download](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp oceanbase-client-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

### MySQL Mode

| Mysql Data type                                                                                | SeaTunnel Data type                                                                                                                |
|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>TINYINT(1)                                                                          | BOOLEAN                                                                                                                            |
| TINYINT                                                                                        | BYTE                                                                                                                               |
| TINYINT<br/>TINYINT UNSIGNED                                                                   | SMALLINT                                                                                                                           |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR            | INT                                                                                                                                |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                   | BIGINT                                                                                                                             |
| BIGINT UNSIGNED                                                                                | DECIMAL(20,0)                                                                                                                      |
| DECIMAL(x,y) (column size < 38)                                                                | DECIMAL(x,y)                                                                                                                       |
| DECIMAL(x,y) (column size >= 38)                                                               | DECIMAL(38,18)                                                                                                                     |
| DECIMAL UNSIGNED                                                                               | DECIMAL((column size + 1), (right-of-decimal digits))                                                                             |
| FLOAT<br/>FLOAT UNSIGNED                                                                       | FLOAT                                                                                                                              |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                     | DOUBLE                                                                                                                             |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM             | STRING                                                                                                                             |
| DATE                                                                                           | DATE                                                                                                                               |
| TIME                                                                                           | TIME                                                                                                                               |
| DATETIME<br/>TIMESTAMP                                                                         | TIMESTAMP                                                                                                                          |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINARY<br/>BIT(n)<br/>GEOMETRY  | BYTES                                                                                                                              |

### Oracle Mode

| Oracle Data type                                                                                | SeaTunnel Data type |
|-------------------------------------------------------------------------------------------------|---------------------|
| Integer                                                                                         | DECIMAL(38,0)       |
| Number(p), p <= 9                                                                               | INT                 |
| Number(p), p <= 18                                                                              | BIGINT              |
| Number(p), p > 18                                                                               | DECIMAL(38,18)      |
| Number(p,s)                                                                                     | DECIMAL(p,s)        |
| Float                                                                                           | DECIMAL(38,18)      |
| REAL<br/>BINARY_FLOAT                                                                           | FLOAT               |
| BINARY_DOUBLE                                                                                   | DOUBLE              |
| CHAR<br/>NCHAR<br/>VARCHAR<br/>VARCHAR2<br/>NVARCHAR2<br/>NCLOB<br/>CLOB<br/>LONG<br/>XML<br/>ROWID | STRING              |
| DATE                                                                                            | TIMESTAMP           |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE                                                    | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                                                             | BYTES               |
| UNKNOWN                                                                                         | Not supported yet   |

## Source Options

| Name                                  | Type       | Required | Default          | Description                                                                                                                                                                                              |
|---------------------------------------|------------|----------|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                   | String     | Yes      | -                | The URL of the JDBC connection, for example `jdbc:oceanbase://localhost:2883/test`.                                                                                                                       |
| driver                                | String     | Yes      | -                | The JDBC class name used to connect to the remote data source. Must be `com.oceanbase.jdbc.Driver`.                                                                                                       |
| username                              | String     | No       | -                | Connection instance username.                                                                                                                                                                            |
| password                              | String     | No       | -                | Connection instance password.                                                                                                                                                                            |
| compatible_mode                       | String     | Yes      | -                | The compatible mode of OceanBase. Must be `mysql` or `oracle`.                                                                                                                                          |
| query                                 | String     | No       | -                | Query statement. Configure one of `query`, `table_path`, or `table_list`.                                                                                                                                |
| table_path                            | String     | No       | -                | Full table path used instead of `query`, for example `test.source`.                                                                                                                                     |
| table_list                            | Array      | No       | -                | List of tables to read. Use it for multi-table reads. Each item can contain `table_path`, `query`, `partition_column`, and other table-level settings.                                                   |
| where_condition                       | String     | No       | -                | Common row filter for all tables or queries. Must start with `where`, for example `where id > 100`.                                                                                                      |
| connection_check_timeout_sec          | Int        | No       | 30               | Time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                          |
| partition_column                      | String     | No       | -                | Column name for parallelism partitioning. Only numeric or string columns are supported.                                                                                                                  |
| partition_lower_bound                 | BigDecimal | No       | -                | Minimum value for `partition_column`. If not set, SeaTunnel queries the database to get it.                                                                                                              |
| partition_upper_bound                 | BigDecimal | No       | -                | Maximum value for `partition_column`. If not set, SeaTunnel queries the database to get it.                                                                                                              |
| partition_num                         | Int        | No       | job parallelism  | Number of partitions. Only positive integers are supported. When reading by `table_path`, prefer `split.size` to control split size.                                                                    |
| fetch_size                            | Int        | No       | 0                | Row fetch size used in queries. `0` means use the JDBC default value.                                                                                                                                   |
| split.size                            | Int        | No       | 8096             | Number of rows in one split when reading by `table_path`.                                                                                                                                               |
| split.even-distribution.factor.lower-bound | Double | No      | 0.05             | Lower bound used to judge whether split-key values are evenly distributed.                                                                                                                              |
| split.even-distribution.factor.upper-bound | Double | No      | 100              | Upper bound used to judge whether split-key values are evenly distributed.                                                                                                                              |
| split.sample-sharding.threshold       | Int        | No       | 1000             | Estimated shard count threshold that triggers sample-based sharding for uneven data.                                                                                                                    |
| split.inverse-sampling.rate           | Int        | No       | 1000             | Sampling rate denominator used by sample-based sharding.                                                                                                                                                |
| split.allow-sampling                  | Boolean    | No       | true             | Whether to allow sample-based sharding.                                                                                                                                                                 |
| split.string_split_mode               | String     | No       | sample           | String split algorithm. Available values: `sample`, `charset_based`.                                                                                                                                   |
| split.string-strategy                 | String     | No       | -                | String partition strategy. Available values: `none`, `hash`, `range`, `auto`.                                                                                                                           |
| split.string_split_mode_collate       | String     | No       | -                | Collation used when `split.string_split_mode = charset_based`.                                                                                                                                          |
| use_select_count                      | Boolean    | No       | false            | Use `SELECT COUNT(*)` during dynamic chunk split. Mainly used by Oracle-compatible read scenarios.                                                                                                      |
| skip_analyze                          | Boolean    | No       | false            | Skip table count analysis during dynamic chunk split. Mainly used by Oracle-compatible read scenarios.                                                                                                  |
| use_regex                             | Boolean    | No       | false            | Treat `table_path` as a regular expression when matching tables.                                                                                                                                        |
| decimal_type_narrowing                | Boolean    | No       | true             | In Oracle-compatible mode, narrow decimal values to `INT` or `BIGINT` when it can be done without losing precision.                                                                                     |
| int_type_narrowing                    | Boolean    | No       | true             | In MySQL-compatible mode, narrow `TINYINT(1)` to `BOOLEAN` when it can be done without losing precision.                                                                                                |
| dialect                               | String     | No       | -                | Appointed JDBC dialect. OceanBase is usually detected from the URL, so this is only needed for special compatibility cases.                                                                             |
| properties                            | Map        | No       | -                | Additional connection configuration parameters. When properties and the URL have the same parameters, the priority depends on the driver implementation; for example, MySQL drivers give `properties` priority over the URL. |
| common-options                        |            | No       | -                | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                       |

### Tips

> Configure one of `query`, `table_path`, or `table_list`.
>
> If `partition_column` is not set and SeaTunnel cannot find a suitable primary
> key or unique key from the table metadata, the source runs with one reader.
> If a supported split column is available, SeaTunnel can read in parallel.
>
> For OceanBase MySQL mode, JDBC connection URLs usually include MySQL-compatible
> parameters such as `rewriteBatchedStatements=true`. For OceanBase Oracle mode,
> use the Oracle-compatible tenant and set `compatible_mode = "oracle"`.

## Task Example

### Simple

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
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

> Read your query table in parallel with the shard field you configured and the
> shard data. Use this when you want to read the whole table.

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
    partition_column = "id"
    partition_num = 10
  }
}

sink {
  Console {}
}
```

### Parallel Boundary

> It is more efficient to read your data source according to the upper and
> lower boundaries you configured.

```hocon
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
    partition_column = "id"
    partition_num = 10
    partition_lower_bound = 1
    partition_upper_bound = 500
  }
}
```

### Table Path

Use `table_path` when you want SeaTunnel to discover table metadata and split
the table automatically.

```hocon
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    table_path = "test.source"
    split.size = 8096
  }
}
```

### Oracle-Compatible Mode

```hocon
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/TESTUSER"
    username = "TESTUSER@test"
    password = ""
    compatible_mode = "oracle"
    query = "SELECT ID, NAME, CREATE_TIME FROM SOURCE"
  }
}
```

### Multiple Table Read

```hocon
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    table_list = [
      { table_path = "test.source_1" },
      { table_path = "test.source_2" }
    ]
    where_condition = "where id > 100"
  }
}
```

## Changelog

<ChangeLog />