import ChangeLog from '../changelog/connector-jdbc.md';

# MySQL

> JDBC MySQL Source Connector

## Description

Read data from MySQL through JDBC. The connector inherits the option set of the generic
[Jdbc source connector](./Jdbc.md) and uses the official MySQL driver (`com.mysql.cj.jdbc.Driver`).

It supports Batch mode (parallel reads with split keys) and CDC reads via the
[MySQL-CDC source connector](./MySQL-CDC.md). For incremental snapshot or change data capture
semantics, prefer the MySQL-CDC connector.

## Support MySQL Version

- 5.5 / 5.6 / 5.7 / 8.0 / 8.1 / 8.2 / 8.3 / 8.4

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table reading](../../introduction/concepts/connector-v2-features.md)

> Supports query SQL and can achieve projection effect. For change data capture, use the
> [MySQL-CDC connector](./MySQL-CDC.md).

## Supported DataSource Info

| Datasource | Supported versions                    | Driver                | Url                              | Maven                                                                       |
|------------|---------------------------------------|-----------------------|----------------------------------|-----------------------------------------------------------------------------|
| MySQL      | Different dependency version has different driver class. | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | [Download](https://mvnrepository.com/artifact/mysql/mysql-connector-java)    |

## Data Type Mapping

|                                        MySQL Data Type                                        |                                                                 SeaTunnel Data Type                                                                |
|-----------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>TINYINT(1)                                                                         | BOOLEAN                                                                                                                                            |
| TINYINT                                                                                       | BYTE                                                                                                                                               |
| TINYINT UNSIGNED<br/>SMALLINT                                                                 | SMALLINT                                                                                                                                           |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR           | INT                                                                                                                                                |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                  | BIGINT                                                                                                                                             |
| BIGINT UNSIGNED                                                                               | DECIMAL(20,0)                                                                                                                                      |
| DECIMAL(x,y)(Get the designated column's specified column size.<38)                           | DECIMAL(x,y)                                                                                                                                       |
| DECIMAL(x,y)(Get the designated column's specified column size.>38)                           | DECIMAL(38,18)                                                                                                                                     |
| DECIMAL UNSIGNED                                                                              | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.)) |
| FLOAT<br/>FLOAT UNSIGNED                                                                      | FLOAT                                                                                                                                              |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                    | DOUBLE                                                                                                                                             |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM          | STRING                                                                                                                                             |
| DATE                                                                                          | DATE                                                                                                                                               |
| TIME(s)                                                                                       | TIME(s)                                                                                                                                            |
| DATETIME<br/>TIMESTAMP(s)                                                                     | TIMESTAMP(s)                                                                                                                                       |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)<br/>GEOMETRY | BYTES                                                                                                                                              |

## Source Options

The MySQL source connector uses the same options as the [Jdbc source connector](./Jdbc.md#source-options).
The options below describe every option exposed by this connector; anything not listed follows the
generic JDBC source behaviour.

| Name                                       | Type       | Required | Default         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------------------------------------------|------------|----------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: `jdbc:mysql://localhost:3306/test`. Add `serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true` for non-trivial loads.                                                                                                                                                                                                                                                                                                                                                                                                      |
| driver                                     | String     | Yes      | -               | The jdbc class name used to connect to the remote data source. For MySQL use `com.mysql.cj.jdbc.Driver`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| username                                   | String     | Yes      | -               | Connection instance user name. `user` is also accepted as a fallback alias.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| password                                   | String     | Yes      | -               | Connection instance password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| query                                      | String     | No       | -               | Query statement. Required when neither `table_path` nor `table_list` is configured.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| connection_check_timeout_sec               | Int        | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| partition_column                           | String     | No       | -               | The column name for parallelism's partition. Only numeric primary key columns are supported, and only one column can be configured.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| partition_lower_bound                      | BigDecimal | No       | -               | The `partition_column` minimum value for the scan. If not set, SeaTunnel queries the database for the minimum.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| partition_upper_bound                      | BigDecimal | No       | -               | The `partition_column` maximum value for the scan. If not set, SeaTunnel queries the database for the maximum.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| partition_num                              | Int        | No       | job parallelism | The number of partitions, only positive integers are supported. The default value matches the job parallelism.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| fetch_size                                 | Int        | No       | 0               | For queries that return a large number of objects, configure the row fetch size used in the query to improve performance by reducing the number of database hits required to satisfy the selection criteria. Zero means use the JDBC driver default value.                                                                                                                                                                                                                                                                                                                                                                |
| properties                                 | Map        | No       | -               | Additional connection configuration parameters. When `properties` and the URL contain the same parameter, the priority is determined by the specific implementation of the driver; in MySQL, `properties` take precedence over the URL.                                                                                                                                                                                                                                                                                                                                                                                |
| use_regex                                  | Boolean    | No       | false           | Control regular expression matching for `table_path`. When `true`, `table_path` is treated as a regular expression pattern. When `false` or not specified, `table_path` is treated as an exact path (no regex matching).                                                                                                                                                                                                                                                                                                                                                                                            |
| table_path                                 | String     | No       | -               | The full path of a table; can be used instead of `query`. Example: `"testdb.table1"`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| table_list                                 | Array      | No       | -               | The list of tables to be read; can be used instead of `table_path`. Example: `[{table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select id, name from testdb.table2"}]`.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| where_condition                            | String     | No       | -               | Common row filter conditions for all tables/queries, must start with `where`, for example `where id > 100`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| split.size                                 | Int        | No       | 8096            | The split size (number of rows) of a table. Captured tables are split into multiple splits when read.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| split.even-distribution.factor.lower-bound | Double     | No       | 0.05            | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., `(MAX(id) - MIN(id) + 1) / row count`), the table chunks are optimized for even distribution. Otherwise, the table is treated as unevenly distributed and the sampling-based sharding strategy is used when the estimated shard count exceeds `sample-sharding.threshold`.                                                                                |
| split.even-distribution.factor.upper-bound | Double     | No       | 100.0           | The upper bound of the chunk key distribution factor. If the distribution factor is calculated to be less than or equal to this upper bound, the table chunks are optimized for even distribution. Otherwise, the table is treated as unevenly distributed and the sampling-based sharding strategy is used when the estimated shard count exceeds `sample-sharding.threshold`.                                                                                                                                                                                          |
| split.sample-sharding.threshold            | Int        | No       | 1000            | The estimated shard count threshold that triggers the sample sharding strategy. When the distribution factor is outside `split.even-distribution.factor.upper-bound` and `split.even-distribution.factor.lower-bound`, and the estimated shard count (approximate row count / split size) exceeds this threshold, the sample sharding strategy is used.                                                                                                                                                                                                              |
| split.allow-sampling                       | Boolean    | No       | true            | Whether to allow sampling-based sharding for uneven split keys. When `false`, SeaTunnel falls back to iterative uneven chunk splitting.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| use_select_count                           | Boolean    | No       | false           | Whether to use `select count(*)` to estimate table row count before splitting.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| skip_analyze                               | Boolean    | No       | false           | Whether to skip table row-count analysis before splitting.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| split.inverse-sampling.rate                | Int        | No       | 1000            | The inverse of the sampling rate used in the sample sharding strategy. For example, a value of 1000 means a 1/1000 sampling rate is applied during sampling. Useful when dealing with very large datasets where a lower sampling rate is preferred.                                                                                                                                                                                                                                                                                                                                                                      |
| int_type_narrowing                         | Boolean    | No       | true            | Int type narrowing. When `true`, `tinyint(1)` is narrowed to `boolean` if there is no loss of precision. Only supported by MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| common-options                             |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

### int_type_narrowing

`int_type_narrowing` controls how MySQL's `tinyint(1)` is mapped to a SeaTunnel type:

- `int_type_narrowing = true` (default): `tinyint(1)` → `boolean`.
- `int_type_narrowing = false`: `tinyint(1)` → `tinyint`.

## Parallel Reader

The JDBC source connector supports parallel reading of data from tables. SeaTunnel splits the data in
the table according to certain rules and hands each split to a reader. The number of readers is
determined by the `parallelism` option.

**Split Key Rules:**

1. If `partition_column` is not null, it is used to calculate the split. The column must be in the
   **Supported split data type** list.
2. If `partition_column` is null, SeaTunnel reads the schema from the table and picks the Primary Key
   or Unique Index. When there are multiple columns in the Primary Key and Unique Index, the first
   column that is in the **supported split data type** list is used. For example, if a table has
   `Primary Key(guid, name varchar)`, `guid` is not in the supported split data type list, so the
   column `name` is used to split the data.

**Supported split data type:**

- String
- Number (`int`, `bigint`, `decimal`, ...)
- Date

## Tips

> If the table cannot be split (for example, the table has no Primary Key or Unique Index and
> `partition_column` is not set), it runs in single concurrency.
>
> Use `table_path` to replace `query` for single-table reading. To read multiple tables, use
> `table_list`.
>
> When inferring a primary key based on a `query`, the key is inherited from the underlying table
> where the first column in the result set is located, and its strictness for the overall join
> result set is not guaranteed (for example, when the query contains joins or reads from multiple
> tables).
>
> For incremental snapshot or change data capture semantics, use the [MySQL-CDC source connector](./MySQL-CDC.md).

## Task Example

### Simple

> This example queries 16 rows from the `type_bin` table in your test database in single parallel and
> selects all fields. You can also restrict which fields are projected to the sink.

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    query = "select * from type_bin limit 16"
  }
}

transform {
}

sink {
  Console {}
}
```

### Parallel by `partition_column`

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    query = "select * from type_bin"
    partition_column = "id"
    split.size = 10000
    # Read start boundary; if omitted SeaTunnel queries the database for the minimum
    # partition_lower_bound = ...
    # Read end boundary; if omitted SeaTunnel queries the database for the maximum
    # partition_upper_bound = ...
  }
}

sink {
  Console {}
}
```

### Parallel by Primary Key or Unique Index

> Configuring `table_path` turns on automatic split. Adjust the strategy with the `split.*` options.

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    table_path = "testdb.table1"
    query = "select * from testdb.table1"
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### Parallel Boundary

> Specifying both the upper and lower bounds of the query range is the most efficient way to drive
> a parallel read.

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    query = "select * from type_bin"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 500
    partition_num = 10
    properties {
      useSSL = "false"
    }
  }
}

sink {
  Console {}
}
```

### Multiple Table Read

> Configuring `table_list` turns on automatic split. Adjust the strategy with the `split.*` options.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"

    table_list = [
      {
        table_path = "testdb.table1"
      },
      {
        table_path = "testdb.table2"
        # Use query to filter rows and columns
        query = "select id, name from testdb.table2 where id > 100"
      }
    ]
    # where_condition = "where id > 100"
    # split.size = 8096
    # split.even-distribution.factor.upper-bound = 100
    # split.even-distribution.factor.lower-bound = 0.05
    # split.sample-sharding.threshold = 1000
    # split.inverse-sampling.rate = 1000
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />