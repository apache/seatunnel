# JDBC

> JDBC source connector

## Description

Read external data source data through JDBC.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                         | type   | required | default value   |
|------------------------------|--------|----------|-----------------|
| url                          | String | Yes      | -               |
| driver                       | String | Yes      | -               |
| user                         | String | No       | -               |
| password                     | String | No       | -               |
| query                        | String | Yes      | -               |
| connection_check_timeout_sec | Int    | No       | 30              |
| partition_column             | String | No       | -               |
| partition_upper_bound        | Long   | No       | -               |
| partition_lower_bound        | Long   | No       | -               |
| partition_num                | Int    | No       | job parallelism |
| common-options               |        | No       | -               |


### driver [string]

The jdbc class name used to connect to the remote data source, if you use MySQL the value is com.mysql.cj.jdbc.Driver.
Warn: for license compliance, you have to provide MySQL JDBC driver yourself, e.g. copy mysql-connector-java-xxx.jar to
$SEATNUNNEL_HOME/lib for Standalone.

### user [string]

userName

### password [string]

password

### url [string]

The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test

### query [string]

Query statement

### connection_check_timeout_sec [int]

The time in seconds to wait for the database operation used to validate the connection to complete.

### partition_column [string]

The column name for parallelism's partition, only support numeric type.

### partition_upper_bound [long]

The partition_column max value for scan, if not set SeaTunnel will query database get max value.

### partition_lower_bound [long]

The partition_column min value for scan, if not set SeaTunnel will query database get min value.

### partition_num [int]

The number of partition count, only support positive integer. default value is job parallelism

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## tips

If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed
in parallel according to the concurrency of tasks.

## appendix

there are some reference value for params above.

| datasource | driver                                       | url                                                          | maven                                                        |
| ---------- | -------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| mysql      | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                             | https://mvnrepository.com/artifact/mysql/mysql-connector-java |
| postgresql | org.postgresql.Driver                        | jdbc:postgresql://localhost:5432/postgres                    | https://mvnrepository.com/artifact/org.postgresql/postgresql |
| dm         | dm.jdbc.driver.DmDriver                      | jdbc:dm://localhost:5236                                     | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18 |
| phoenix    | org.apache.phoenix.queryserver.client.Driver | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client |
| sqlserver  | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:microsoft:sqlserver://localhost:1433                    | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc |
| oracle     | oracle.jdbc.OracleDriver                     | jdbc:oracle:thin:@localhost:1521/xepdb1                      | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |
| gbase8a    | com.gbase.jdbc.Driver                        | jdbc:gbase://e2e_gbase8aDb:5258/test                         | https://www.gbase8.cn/wp-content/uploads/2020/10/gbase-connector-java-8.3.81.53-build55.5.7-bin_min_mix.jar |
| starrocks  | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                             | https://mvnrepository.com/artifact/mysql/mysql-connector-java |

## Example

simple:
```
    Jdbc {
        url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        query = "select * from type_bin"
    }
```

parallel:

```
    Jdbc {
        url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        query = "select * from type_bin"
        partition_column = "id"
        partition_num = 10
    }
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add ClickHouse Source Connector

### 2.3.0-beta 2022-10-20

- [Feature] Support Phoenix JDBC Source ([2499](https://github.com/apache/incubator-seatunnel/pull/2499))
- [Feature] Support SQL Server JDBC Source ([2646](https://github.com/apache/incubator-seatunnel/pull/2646))
- [Feature] Support Oracle JDBC Source ([2550](https://github.com/apache/incubator-seatunnel/pull/2550))
- [Feature] Support StarRocks JDBC Source ([3060](https://github.com/apache/incubator-seatunnel/pull/3060))
- [Feature] Support GBase8a JDBC Source ([3026](https://github.com/apache/incubator-seatunnel/pull/3026))
