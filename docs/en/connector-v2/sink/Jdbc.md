# JDBC

> JDBC sink connector

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

:::tip

Warn: for license compliance, you have to provide database driver yourself, copy to `$SEATNUNNEL_HOME/lib/` directory in order to make them work.

e.g. If you use MySQL, should download and copy `mysql-connector-java-xxx.jar` to `$SEATNUNNEL_HOME/lib/`. For Spark/Flink, you should also copy it to `$SPARK_HOME/jars/` or `$FLINK_HOME/lib/`.

:::

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

- [x] [cdc](../../concept/connector-v2-features.md)

## Options

|                   name                    |  type   | required | default value |
|-------------------------------------------|---------|----------|---------------|
| url                                       | String  | Yes      | -             |
| driver                                    | String  | Yes      | -             |
| user                                      | String  | No       | -             |
| password                                  | String  | No       | -             |
| query                                     | String  | No       | -             |
| compatible_mode                           | String  | No       | -             |
| database                                  | String  | No       | -             |
| table                                     | String  | No       | -             |
| primary_keys                              | Array   | No       | -             |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false         |
| connection_check_timeout_sec              | Int     | No       | 30            |
| max_retries                               | Int     | No       | 0             |
| batch_size                                | Int     | No       | 1000          |
| is_exactly_once                           | Boolean | No       | false         |
| generate_sink_sql                         | Boolean | No       | false         |
| xa_data_source_class_name                 | String  | No       | -             |
| max_commit_attempts                       | Int     | No       | 3             |
| transaction_timeout_sec                   | Int     | No       | -1            |
| auto_commit                               | Boolean | No       | true          |
| common-options                            |         | no       | -             |

### driver [string]

The jdbc class name used to connect to the remote data source, if you use MySQL the value is `com.mysql.cj.jdbc.Driver`.

### user [string]

userName

### password [string]

password

### url [string]

The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test

### query [string]

Use this sql write upstream input datas to database. e.g `INSERT ...`

### compatible_mode [string]

The compatible mode of database, required when the database supports multiple compatible modes. For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'.

Postgres 9.5 version or below,please set it to `postgresLow` to support cdc

### database [string]

Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.

This option is mutually exclusive with `query` and has a higher priority.

### table [string]

Use `database` and this `table-name` auto-generate sql and receive upstream input datas write to database.

This option is mutually exclusive with `query` and has a higher priority.

### primary_keys [array]

This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.

### support_upsert_by_query_primary_key_exist [boolean]

Choose to use INSERT sql, UPDATE sql to process update events(INSERT, UPDATE_AFTER) based on query primary key exists. This configuration is only used when database unsupported upsert syntax.
**Note**: that this method has low performance

### connection_check_timeout_sec [int]

The time in seconds to wait for the database operation used to validate the connection to complete.

### max_retries[int]

The number of retries to submit failed (executeBatch)

### batch_size[int]

For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`
, the data will be flushed into the database

### is_exactly_once[boolean]

Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to
set `xa_data_source_class_name`.

### generate_sink_sql[boolean]

Generate sql statements based on the database table you want to write to

### xa_data_source_class_name[string]

The xa data source class name of the database Driver, for example, mysql is `com.mysql.cj.jdbc.MysqlXADataSource`, and
please refer to appendix for other data sources

### max_commit_attempts[int]

The number of retries for transaction commit failures

### transaction_timeout_sec[int]

The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect
exactly-once semantics

### auto_commit [boolean]

Automatic transaction commit is enabled by default

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## tips

In the case of is_exactly_once = "true", Xa transactions are used. This requires database support, and some databases require some setup :
1 postgres needs to set `max_prepared_transactions > 1` such as `ALTER SYSTEM set max_prepared_transactions to 10`.
2 mysql version need >= `8.0.29` and Non-root users need to grant `XA_RECOVER_ADMIN` permissions. such as `grant XA_RECOVER_ADMIN on test_db.* to 'user1'@'%'`.
3 mysql can try to add `rewriteBatchedStatements=true` parameter in url for better performance.

## appendix

there are some reference value for params above.

| datasource |                    driver                    |                                url                                 |             xa_data_source_class_name              |                                                    maven                                                    |
|------------|----------------------------------------------|--------------------------------------------------------------------|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| MySQL      | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | com.mysql.cj.jdbc.MysqlXADataSource                | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                               |
| PostgreSQL | org.postgresql.Driver                        | jdbc:postgresql://localhost:5432/postgres                          | org.postgresql.xa.PGXADataSource                   | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                |
| DM         | dm.jdbc.driver.DmDriver                      | jdbc:dm://localhost:5236                                           | dm.jdbc.driver.DmdbXADataSource                    | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                |
| Phoenix    | org.apache.phoenix.queryserver.client.Driver | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF | /                                                  | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                        |
| SQL Server | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433                                    | com.microsoft.sqlserver.jdbc.SQLServerXADataSource | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                       |
| Oracle     | oracle.jdbc.OracleDriver                     | jdbc:oracle:thin:@localhost:1521/xepdb1                            | oracle.jdbc.xa.OracleXADataSource                  | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                          |
| sqlite     | org.sqlite.JDBC                              | jdbc:sqlite:test.db                                                | /                                                  | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                   |
| GBase8a    | com.gbase.jdbc.Driver                        | jdbc:gbase://e2e_gbase8aDb:5258/test                               | /                                                  | https://www.gbase8.cn/wp-content/uploads/2020/10/gbase-connector-java-8.3.81.53-build55.5.7-bin_min_mix.jar |
| StarRocks  | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                               |
| db2        | com.ibm.db2.jcc.DB2Driver                    | jdbc:db2://localhost:50000/testdb                                  | com.ibm.db2.jcc.DB2XADataSource                    | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                           |
| saphana    | com.sap.db.jdbc.Driver                       | jdbc:sap://localhost:39015                                         | /                                                  | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                              |
| Doris      | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                               |
| teradata   | com.teradata.jdbc.TeraDriver                 | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test              | /                                                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                               |
| Redshift   | com.amazon.redshift.jdbc42.Driver            | jdbc:redshift://localhost:5439/testdb                              | com.amazon.redshift.xa.RedshiftXADataSource        | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                      |
| Snowflake  | net.snowflake.client.jdbc.SnowflakeDriver    | jdbc:snowflake://<account_name>.snowflakecomputing.com             | /                                                  | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                             |
| Vertica    | com.vertica.jdbc.Driver                      | jdbc:vertica://localhost:5433                                      | /                                                  | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar             |
| OceanBase  | com.oceanbase.jdbc.Driver                    | jdbc:oceanbase://localhost:2881                                    | /                                                  | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.3/oceanbase-client-2.4.3.jar              |

## Example

Simple

```
jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"
}

```

Exactly-once

```
jdbc {

    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"

    max_retries = 0
    user = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"

    is_exactly_once = "true"

    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
}
```

CDC(Change data capture) event

```
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "123456"
        
        database = "sink_database"
        table = "sink_table"
        primary_keys = ["key1", "key2", ...]
    }
}
```

Postgresql 9.5 version below support CDC(Change data capture) event

```
sink {
    jdbc {
        url = "jdbc:postgresql://localhost:5432"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "123456"
        compatible_mode="postgresLow"
        database = "sink_database"
        table = "sink_table"
        support_upsert_by_query_primary_key_exist = true
        generate_sink_sql = true
        primary_keys = ["key1", "key2", ...]
    }
}

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Console Sink Connector

### 2.3.0-beta 2022-10-20

- [BugFix] Fix JDBC split exception ([2904](https://github.com/apache/seatunnel/pull/2904))
- [Feature] Support Phoenix JDBC Sink ([2499](https://github.com/apache/seatunnel/pull/2499))
- [Feature] Support SQL Server JDBC Sink ([2646](https://github.com/apache/seatunnel/pull/2646))
- [Feature] Support Oracle JDBC Sink ([2550](https://github.com/apache/seatunnel/pull/2550))
- [Feature] Support StarRocks JDBC Sink ([3060](https://github.com/apache/seatunnel/pull/3060))
- [Feature] Support DB2 JDBC Sink ([2410](https://github.com/apache/seatunnel/pull/2410))

### next version

- [Feature] Support CDC write DELETE/UPDATE/INSERT events ([3378](https://github.com/apache/seatunnel/issues/3378))
- [Feature] Support Teradata JDBC　Sink ([3362](https://github.com/apache/seatunnel/pull/3362))
- [Feature] Support Sqlite JDBC Sink ([3089](https://github.com/apache/seatunnel/pull/3089))
- [Feature] Support CDC write DELETE/UPDATE/INSERT events ([3378](https://github.com/apache/seatunnel/issues/3378))
- [Feature] Support Doris JDBC Sink
- [Feature] Support Redshift JDBC Sink([#3615](https://github.com/apache/seatunnel/pull/3615))
- [Improve] Add config item enable upsert by query([#3708](https://github.com/apache/seatunnel/pull/3708))
- [Improve] Add database field to sink config([#4199](https://github.com/apache/seatunnel/pull/4199))
- [Improve] Add Vertica connector([#4303](https://github.com/apache/seatunnel/pull/4303))

