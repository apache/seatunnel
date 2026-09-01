import ChangeLog from '../changelog/connector-jdbc.md';

# DB2

> JDBC DB2 Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read data from DB2 through JDBC. DB2 requires the IBM `db2jcc` driver; SeaTunnel does not ship it for licensing reasons. Use the `Jdbc` plugin name in the source block and set `driver = "com.ibm.db2.jcc.DB2Driver"`.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

> Supports query SQL and can achieve column projection.

## Supported DataSource Info

| Datasource | Supported versions                                   | Driver                  | Url                           | Maven                                                              |
|------------|------------------------------------------------------|-------------------------|-------------------------------|--------------------------------------------------------------------|
| DB2        | Different dependency version has different driver class. | com.ibm.db2.jcc.DB2Driver | jdbc:db2://127.0.0.1:50000/dbname | [Download](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example DB2 datasource: cp db2-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                                            DB2 Data Type                                             | SeaTunnel Data Type |
|------------------------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                                              | BOOLEAN             |
| SMALLINT                                                                                             | SHORT               |
| INT<br/>INTEGER<br/>                                                                                 | INTEGER             |
| BIGINT                                                                                               | LONG                |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM                                                                  | DECIMAL(38,18)      |
| REAL                                                                                                 | FLOAT               |
| FLOAT<br/>DOUBLE<br/>DOUBLE PRECISION<br/>DECFLOAT                                                   | DOUBLE              |
| CHAR<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>LONG VARGRAPHIC<br/>DBCLOB | STRING              |
| BLOB                                                                                                 | BYTES               |
| DATE                                                                                                 | DATE                |
| TIME                                                                                                 | TIME                |
| TIMESTAMP                                                                                            | TIMESTAMP           |
| ROWID<br/>XML                                                                                        | Not supported yet   |

## Source Options

|             Name             |    Type    | Required | Default | Description                                                                                                                                                                |
|------------------------------|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -       | JDBC connection URL, for example `jdbc:db2://127.0.0.1:50000/dbname`.                                                                                                     |
| driver                       | String     | Yes      | -       | JDBC driver class name. Use `com.ibm.db2.jcc.DB2Driver` for DB2.                                                                                                           |
| username                     | String     | No       | -       | Username for the DB2 instance. `user` is also accepted as a fallback key for `username`.                                                                                    |
| password                     | String     | No       | -       | Password for the DB2 instance.                                                                                                                                             |
| query                        | String     | Yes      | -       | SELECT statement used to read data. The column list of the SELECT defines the output schema; select only the columns you need.                                              |
| connection_check_timeout_sec | Int        | No       | 30      | Seconds to wait for the connection check before failing.                                                                                                                   |
| partition_column             | String     | No       | -       | Column used to split data for parallel reading. Supports numeric columns and string columns (with `split.string_split_mode`); only one column can be configured.          |
| partition_lower_bound        | String     | No       | -       | Lower bound of `partition_column` for range splitting. If not set, SeaTunnel queries the minimum value.                                                                    |
| partition_upper_bound        | String     | No       | -       | Upper bound of `partition_column` for range splitting. If not set, SeaTunnel queries the maximum value.                                                                    |
| partition_num                | Int        | No       | 10      | Number of source splits used in parallel reading. Defaults to `10`. Increase this value if `env.parallelism` is larger and you want one split per reader task.              |
| fetch_size                   | Int        | No       | 0       | JDBC fetch size for the query. `0` means use the JDBC driver default. Use a positive value to reduce database round-trips for large result sets.                            |
| properties                   | Map        | No       | -       | Extra JDBC connection properties. When the same key appears in both `properties` and `url`, the precedence is driver-specific.                                             |
| common-options               |            | No       | -       | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                          |

### Tips

> If `partition_column` is not set, the source reads with one split. If it is set, SeaTunnel splits the table into exactly `partition_num` (default 10) splits regardless of `env.parallelism`; the number of splits that read concurrently is then bounded by `min(partition_num, env.parallelism)` — extra reader slots either sit idle (when `parallelism > partition_num`) or pick up more than one split sequentially (when `parallelism < partition_num`). The two are independent, not "the greater of the two."

## Task Example

### Simple

This example queries all fields of `type_bin` in DB2 with two parallel readers and prints them to the console.

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    connection_check_timeout_sec = 100
    username = "db2inst1"
    password = "123456"
    query = "select * from type_bin"
  }
}

sink {
  Console {}
}
```

### Parallel Reading By Numeric Column

Read the table in parallel by a numeric `partition_column` and let SeaTunnel pick the lower and upper bounds for you.

```hocon
source {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    query = "select * from type_bin"
    partition_column = "id"
    partition_num = 10
  }
}
```

### Parallel Reading With Explicit Bounds

Provide explicit `partition_lower_bound` and `partition_upper_bound` to avoid the extra `MIN`/`MAX` query SeaTunnel otherwise issues to learn the column range.

```hocon
source {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    query = "select * from type_bin"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 500
    partition_num = 10
  }
}
```

## Changelog

<ChangeLog />
