# JDBC

> JDBC source connector

## Support those engines

> Spark <br>
> Flink <br>
> Seatunnel Zeta <br>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Read external data source data through JDBC.

## Supported DataSource list

| datasource |          supported versions          |                       driver                        |                                  url                                   |                                                          maven                                                          |
|------------|--------------------------------------|-----------------------------------------------------|------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| mysql      | support version >= 5.7x and <8.0.26x | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | [Download](https://mvnrepository.com/artifact/mysql/mysql-connector-java)                                               |
| postgresql | support all version                  | org.postgresql.Driver                               | jdbc:postgresql://localhost:5432/postgres                              | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql)                                                |
| dm         | support version >=XXX and <XXX       | dm.jdbc.driver.DmDriver                             | jdbc:dm://localhost:5236                                               | [Download](https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18)                                                |
| phoenix    | support version >=XXX and <XXX       | org.apache.phoenix.queryserver.client.Driver        | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF     | [Download](https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client)                        |
| sqlserver  | support version >=XXX and <XXX       | com.microsoft.sqlserver.jdbc.SQLServerDriver        | jdbc:sqlserver://localhost:1433                                        | [Download](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc)                                       |
| oracle     | support version >=XXX and <XXX       | oracle.jdbc.OracleDriver                            | jdbc:oracle:thin:@localhost:1521/xepdb1                                | [Download](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8)                                          |
| sqlite     | support version >=XXX and <XXX       | org.sqlite.JDBC                                     | jdbc:sqlite:test.db                                                    | [Download](https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc)                                                   |
| gbase8a    | support version >=XXX and <XXX       | com.gbase.jdbc.Driver                               | jdbc:gbase://e2e_gbase8aDb:5258/test                                   | [Download](https://www.gbase8.cn/wp-content/uploads/2020/10/gbase-connector-java-8.3.81.53-build55.5.7-bin_min_mix.jar) |
| starrocks  | support version >=XXX and <XXX       | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | [Download](https://mvnrepository.com/artifact/mysql/mysql-connector-java)                                               |
| db2        | support version >=XXX and <XXX       | com.ibm.db2.jcc.DB2Driver                           | jdbc:db2://localhost:50000/testdb                                      | [Download](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4)                                           |
| tablestore | support version >=XXX and <XXXe      | com.alicloud.openservices.tablestore.jdbc.OTSDriver | "jdbc:ots:http s://myinstance.cn-hangzhou.ots.aliyuncs.com/myinstance" | [Download](https://mvnrepository.com/artifact/com.aliyun.openservices/tablestore-jdbc)                                  |
| saphana    | support version >=XXX and <XXX       | com.sap.db.jdbc.Driver                              | jdbc:sap://localhost:39015                                             | [Download](https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc)                                              |
| doris      | support version >=XXX and <XXX       | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | [Download](https://mvnrepository.com/artifact/mysql/mysql-connector-java)                                               |
| teradata   | support version >=XXX and <XXX       | com.teradata.jdbc.TeraDriver                        | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test                  | [Download](https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc)                                               |
| Redshift   | support version >=XXX and <XXX       | com.amazon.redshift.jdbc42.Driver                   | jdbc:redshift://localhost:5439/testdb                                  | [Download](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42)                                      |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br>
> For example Mysql datasource: cp mysql-connector-java-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

| Seatunnel Data type |                                      Mysql Data type                                       | xxx Data type | xxx Data type |
|---------------------|--------------------------------------------------------------------------------------------|---------------|---------------|
| BIGINT              | BIGINT<br/>INT UNSIGNED                                                                    | xxx           | xxx           |
| STRING              | VARCHAR(N)<BR/>CHAR(N)<BR/>TEXT<br/>TINYTEXT<br/>MEDIUMTEXT<br/>LONGTEXT<br/>JSON<br/>ENUM | xxx           | xxx           |
| BOOLEAN             | BOOLEAN                                                                                    | xxx           | xxx           |
| TINYINT             | TINYINT                                                                                    | xxx           | xxx           |
| SMALLINT            | SMALLINT<br/>TINYINT UNSIGNED                                                              | xxx           | xxx           |
| INT                 | INT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED                             | xxx           | xxx           |
| BIGINT              | STRING                                                                                     | xxx           | xxx           |
| FLOAT               | FLOAT<br/>FLOAT UNSIGNED                                                                   | xxx           | xxx           |
| DOUBLE              | DOUBLE<br/>DOUBLE UNSIGNED                                                                 | xxx           | xxx           |
| DECIMAL(P, S)       | DECIMAL(P, S)                                                                              | xxx           | xxx           |
| BYTES               | BIT                                                                                        | xxx           | xxx           |
| DATE                | DATE                                                                                       | xxx           | xxx           |
| TIME                | TIME                                                                                       | xxx           | xxx           |
| TIMESTAMP           | DATETIME [(p)]                                                                             | xxx           | xxx           |
| NULL                | NULL                                                                                       | xxx           | xxx           |

## Options

|             name             |  type  | required |     default     |                                                                                                                     Description                                                                                                                     |
|------------------------------|--------|----------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:mysql://localhost:3306/test                                                                                                                                                                   |
| driver                       | String | Yes      | -               | The jdbc class name used to connect to the remote data source, if you use MySQL the value is `com.mysql.cj.jdbc.Driver`.                                                                                                                            |
| user                         | String | No       | -               | Connection instance user name                                                                                                                                                                                                                       |
| password                     | String | No       | -               | Connection instance password                                                                                                                                                                                                                        |
| query                        | String | Yes      | -               | Query statement                                                                                                                                                                                                                                     |
| connection_check_timeout_sec | Int    | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                  |
| partition_column             | String | No       | -               | The column name for parallelism's partition, only support numeric type.                                                                                                                                                                             |
| partition_lower_bound        | Long   | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                    |
| partition_upper_bound        | Long   | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                    |
| partition_num                | Int    | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism                                                                                                                                                      |
| fetch_size                   | Int    | No       | 0               | For queries that return a large number of objects, you can configure the row fetch size used in the query toimprove performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value. |
| common-options               |        | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                             |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### simple:

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

### parallel:

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
- [Feature] Support DB2 JDBC Source ([2410](https://github.com/apache/incubator-seatunnel/pull/2410))

### next version

- [BugFix] Fix jdbc split bug ([3220](https://github.com/apache/incubator-seatunnel/pull/3220))
- [Feature] Support Sqlite JDBC Source ([3089](https://github.com/apache/incubator-seatunnel/pull/3089))
- [Feature] Support Tablestore Source ([3309](https://github.com/apache/incubator-seatunnel/pull/3309))
- [Feature] Support Teradata JDBC　Source ([3362](https://github.com/apache/incubator-seatunnel/pull/3362))
- [Feature] Support JDBC Fetch Size Config ([3478](https://github.com/apache/incubator-seatunnel/pull/3478))
- [Feature] Support Doris JDBC Source ([3586](https://github.com/apache/incubator-seatunnel/pull/3586))
- [Feature] Support Redshift JDBC Sink([#3615](https://github.com/apache/incubator-seatunnel/pull/3615))
- [BugFix] Fix jdbc connection reset bug ([3670](https://github.com/apache/incubator-seatunnel/pull/3670))

