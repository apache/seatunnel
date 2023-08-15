# JDBC

> JDBC source connector

## Description

Read external data source data through JDBC.

:::tip

Warn: for license compliance, you have to provide database driver yourself, copy to `$SEATNUNNEL_HOME/lib/` directory in order to make them work.

e.g. If you use MySQL, should download and copy `mysql-connector-java-xxx.jar` to `$SEATNUNNEL_HOME/lib/`. For Spark/Flink, you should also copy it to `$SPARK_HOME/jars/` or `$FLINK_HOME/lib/`.

:::

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|             name             |  type  | required |  default value  |
|------------------------------|--------|----------|-----------------|
| url                          | String | Yes      | -               |
| driver                       | String | Yes      | -               |
| user                         | String | No       | -               |
| password                     | String | No       | -               |
| query                        | String | Yes      | -               |
| compatible_mode              | String | No       | -               |
| connection_check_timeout_sec | Int    | No       | 30              |
| partition_column             | String | No       | -               |
| partition_upper_bound        | Long   | No       | -               |
| partition_lower_bound        | Long   | No       | -               |
| partition_num                | Int    | No       | job parallelism |
| fetch_size                   | Int    | No       | 0               |
| common-options               |        | No       | -               |

### driver [string]

The jdbc class name used to connect to the remote data source, if you use MySQL the value is `com.mysql.cj.jdbc.Driver`.

### user [string]

userName

### password [string]

password

### url [string]

The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test

### query [string]

Query statement

### compatible_mode [string]

The compatible mode of database, required when the database supports multiple compatible modes. For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'.

### connection_check_timeout_sec [int]

The time in seconds to wait for the database operation used to validate the connection to complete.

### partition_column [string]

The column name for parallelism's partition, only support numeric type.

### partition_upper_bound [BigDecimal]

The partition_column max value for scan, if not set SeaTunnel will query database get max value.

### partition_lower_bound [BigDecimal]

The partition_column min value for scan, if not set SeaTunnel will query database get min value.

### partition_num [int]

The number of partition count, only support positive integer. default value is job parallelism

### fetch_size [int]

For queries that return a large number of objects, you can configure the row fetch size used in the query to
improve performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value.

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## tips

If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed
in parallel according to the concurrency of tasks.

## appendix

there are some reference value for params above.

| datasource |                       driver                        |                                  url                                   |                                                    maven                                                    |
|------------|-----------------------------------------------------|------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| mysql      | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                               |
| postgresql | org.postgresql.Driver                               | jdbc:postgresql://localhost:5432/postgres                              | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                |
| dm         | dm.jdbc.driver.DmDriver                             | jdbc:dm://localhost:5236                                               | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                |
| phoenix    | org.apache.phoenix.queryserver.client.Driver        | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF     | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                        |
| sqlserver  | com.microsoft.sqlserver.jdbc.SQLServerDriver        | jdbc:sqlserver://localhost:1433                                        | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                       |
| oracle     | oracle.jdbc.OracleDriver                            | jdbc:oracle:thin:@localhost:1521/xepdb1                                | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                          |
| sqlite     | org.sqlite.JDBC                                     | jdbc:sqlite:test.db                                                    | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                   |
| gbase8a    | com.gbase.jdbc.Driver                               | jdbc:gbase://e2e_gbase8aDb:5258/test                                   | https://www.gbase8.cn/wp-content/uploads/2020/10/gbase-connector-java-8.3.81.53-build55.5.7-bin_min_mix.jar |
| starrocks  | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                               |
| db2        | com.ibm.db2.jcc.DB2Driver                           | jdbc:db2://localhost:50000/testdb                                      | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                           |
| tablestore | com.alicloud.openservices.tablestore.jdbc.OTSDriver | "jdbc:ots:http s://myinstance.cn-hangzhou.ots.aliyuncs.com/myinstance" | https://mvnrepository.com/artifact/com.aliyun.openservices/tablestore-jdbc                                  |
| saphana    | com.sap.db.jdbc.Driver                              | jdbc:sap://localhost:39015                                             | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                              |
| doris      | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                               |
| teradata   | com.teradata.jdbc.TeraDriver                        | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                               |
| Snowflake  | net.snowflake.client.jdbc.SnowflakeDriver           | jdbc:snowflake://<account_name>.snowflakecomputing.com                 | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                             |
| Redshift   | com.amazon.redshift.jdbc42.Driver                   | jdbc:redshift://localhost:5439/testdb?defaultRowFetchSize=1000         | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                      |
| Vertica    | com.vertica.jdbc.Driver                             | jdbc:vertica://localhost:5433                                          | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar             |
| OceanBase  | com.oceanbase.jdbc.Driver                           | jdbc:oceanbase://localhost:2881                                        | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.3/oceanbase-client-2.4.3.jar              |

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

- [Feature] Support Phoenix JDBC Source ([2499](https://github.com/apache/seatunnel/pull/2499))
- [Feature] Support SQL Server JDBC Source ([2646](https://github.com/apache/seatunnel/pull/2646))
- [Feature] Support Oracle JDBC Source ([2550](https://github.com/apache/seatunnel/pull/2550))
- [Feature] Support StarRocks JDBC Source ([3060](https://github.com/apache/seatunnel/pull/3060))
- [Feature] Support GBase8a JDBC Source ([3026](https://github.com/apache/seatunnel/pull/3026))
- [Feature] Support DB2 JDBC Source ([2410](https://github.com/apache/seatunnel/pull/2410))

### next version

- [BugFix] Fix jdbc split bug ([3220](https://github.com/apache/seatunnel/pull/3220))
- [Feature] Support Sqlite JDBC Source ([3089](https://github.com/apache/seatunnel/pull/3089))
- [Feature] Support Tablestore Source ([3309](https://github.com/apache/seatunnel/pull/3309))
- [Feature] Support Teradata JDBC　Source ([3362](https://github.com/apache/seatunnel/pull/3362))
- [Feature] Support JDBC Fetch Size Config ([3478](https://github.com/apache/seatunnel/pull/3478))
- [Feature] Support Doris JDBC Source ([3586](https://github.com/apache/seatunnel/pull/3586))
- [Feature] Support Redshift JDBC Sink([#3615](https://github.com/apache/seatunnel/pull/3615))
- [BugFix] Fix jdbc connection reset bug ([3670](https://github.com/apache/seatunnel/pull/3670))
- [Improve] Add Vertica connector([#4303](https://github.com/apache/seatunnel/pull/4303))

