import ChangeLog from '../changelog/connector-jdbc.md';

# HiveJdbc

> JDBC Hive Source Connector

## Support Hive Version

- Definitely supports 3.1.3 and 3.1.2, other versions need to be tested.

## Timeout Parameter Support

The `socket_timeout_ms` and `connect_timeout_ms` parameters are tested with **Hive 3.2.0+**. For earlier versions (including 3.1.x), these parameters have not been verified yet. The parameters will be passed to the JDBC driver, but their effectiveness depends on the Hive version being used.

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

> supports query SQL and can achieve projection effect.

## Description

Read data from Apache Hive through the standard JDBC interface. The connector uses the HiveServer2 JDBC
driver (`org.apache.hive.jdbc.HiveDriver`) and submits the configured `query` to fetch rows. Compared to the
[Hive source](Hive.md), which reads files directly from HDFS, HiveJdbc delegates all I/O to HiveServer2 and
is best for cases where direct metastore/HDFS access is not available on the SeaTunnel worker. Kerberos
authentication is supported.

## Supported DataSource Info

| Datasource |                    Supported versions                    |             Driver              |                 Url                  |                                  Maven                                   |
|------------|----------------------------------------------------------|---------------------------------|--------------------------------------|--------------------------------------------------------------------------|
| Hive       | Different dependency version has different driver class. | org.apache.hive.jdbc.HiveDriver | jdbc:hive2://localhost:10000/default | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc) |

## Database Dependency

> For Spark/Flink: place the [Hive JDBC driver](https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc)
> jar in `${SEATUNNEL_HOME}/plugins/jdbc/lib/`. For SeaTunnel Zeta: place it in `${SEATUNNEL_HOME}/lib/`.

## Data Type Mapping

|                                      Hive Data Type                                       | SeaTunnel Data Type |
|-------------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                                   | BOOLEAN             |
| TINYINT<br/> SMALLINT                                                                     | SHORT               |
| INT<br/>INTEGER                                                                           | INT                 |
| BIGINT                                                                                    | LONG                |
| FLOAT                                                                                     | FLOAT               |
| DOUBLE<br/>DOUBLE PRECISION                                                               | DOUBLE              |
| DECIMAL(x,y)<br/>NUMERIC(x,y)<br/>(Get the designated column's specified column size.<38) | DECIMAL(x,y)        |
| DECIMAL(x,y)<br/>NUMERIC(x,y)<br/>(Get the designated column's specified column size.>38) | DECIMAL(38,18)      |
| CHAR<br/>VARCHAR<br/>STRING                                                               | STRING              |
| DATE                                                                                      | DATE                |
| TIMESTAMP                                                                                 | TIMESTAMP           |
| BINARY<br/>ARRAY<br/>INTERVAL<br/>MAP<br/>STRUCT<br/>UNIONTYPE                            | Not supported yet   |

## Source Options

|             Name             |    Type    | Required |     Default     |                                                                                                                            Description                                                                                                                            |
|------------------------------|------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Example: `jdbc:hive2://localhost:10000/default`. Point this at the HiveServer2 endpoint.                                                                                                                                          |
| driver                       | String     | Yes      | -               | The JDBC class name used to connect to the remote data source. For Hive, the value is `org.apache.hive.jdbc.HiveDriver`.                                                                                                                                            |
| username                     | String     | No       | -               | Connection instance user name.                                                                                                                                                                                                                                     |
| password                     | String     | No       | -               | Connection instance password.                                                                                                                                                                                                                                      |
| query                        | String     | Yes      | -               | Query statement. The HiveServer2 result schema determines the output schema.                                                                                                                                                                                       |
| connection_check_timeout_sec | Int        | No       | 30              | The time, in seconds, to wait for the database operation used to validate the connection to complete.                                                                                                                                                              |
| socket_timeout_ms            | Int        | No       | 86400000        | Socket timeout in milliseconds for reading data from the server. Set to `0` for no timeout. Tested with Hive 3.2.0+. For earlier versions, not yet verified.                                                                                                          |
| connect_timeout_ms           | Int        | No       | 86400000        | Connection timeout in milliseconds for establishing a connection to the server. Set to `0` for no timeout. Tested with Hive 3.2.0+. For earlier versions, not yet verified.                                                                                          |
| partition_column             | String     | No       | -               | The column name for parallelism partition. Only supports numeric primary key columns.                                                                                                                                                                             |
| partition_lower_bound        | BigDecimal | No       | -               | The `partition_column` minimum value for the scan. If not set, SeaTunnel queries the database for the minimum value.                                                                                                                                                |
| partition_upper_bound        | BigDecimal | No       | -               | The `partition_column` maximum value for the scan. If not set, SeaTunnel queries the database for the maximum value.                                                                                                                                                |
| partition_num                | Int        | No       | job parallelism | Number of partitions. Only positive integers are supported. Default value is the job parallelism.                                                                                                                                                                  |
| fetch_size                   | Int        | No       | 0               | For queries that return a large number of rows, configure the row fetch size used in the query to improve performance by reducing the number of database hits required to satisfy the selection criteria. `0` means use the JDBC driver default.                       |
| common-options               |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                                  |
| use_kerberos                 | Boolean    | No       | false           | Whether to enable Kerberos authentication.                                                                                                                                                                                                                         |
| kerberos_principal           | String     | No       | -               | When `use_kerberos = true`, set the Kerberos principal, for example `test_user@xxx`.                                                                                                                                                                              |
| kerberos_keytab_path         | String     | No       | -               | When `use_kerberos = true`, set the Kerberos keytab file path, for example `/home/test/test_user.keytab`.                                                                                                                                                          |
| krb5_path                    | String     | No       | /etc/krb5.conf  | When `use_kerberos = true`, set the `krb5.conf` path, for example `/seatunnel/krb5.conf`, or keep the default `/etc/krb5.conf`.                                                                                                                                     |

### Tips

> If `partition_column` is not set, the source runs in single concurrency. When `partition_column` is set, the
> source runs in parallel according to the job parallelism. If the partition column is a large numeric type
> such as `BIGINT` and the data is heavily skewed, set `parallelism = 1` to avoid data skew.

## Task Example

### Simple

> This example queries 16 rows of `type_bin` in your test database in single parallel and queries all of its
> fields. You can also specify which fields to query for final output to the console.

```hocon
# Defining the runtime environment
env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        query = "select * from type_bin limit 16"
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

> Read your query table in parallel with the shard field you configured. Use this pattern when you want to
> read the whole table.

```hocon
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        # Define query logic as required
        query = "select * from type_bin"
        # Parallel sharding reads fields
        partition_column = "id"
        # Number of fragments
        partition_num = 10
    }
}
```

### Parallel Boundary

> It is more efficient to specify the data within the upper and lower bounds of the query. Bound the scanned
> range explicitly when the values are tightly clustered.

```hocon
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        # Define query logic as required
        query = "select * from type_bin"
        partition_column = "id"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

### Read with Kerberos

```hocon
source {
    Jdbc {
        url = "jdbc:hive2://hive-server:10000/default;principal=hive/_HOST@REALM"
        driver = "org.apache.hive.jdbc.HiveDriver"
        query = "select * from type_bin"
        use_kerberos = true
        kerberos_principal = "test_user@REALM"
        kerberos_keytab_path = "/home/test/test_user.keytab"
        krb5_path = "/etc/krb5.conf"
    }
}
```

## Changelog

<ChangeLog />