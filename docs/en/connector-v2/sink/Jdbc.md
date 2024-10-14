# JDBC

> JDBC sink connector

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the jdbc driver jar package has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the jdbc driver jar package has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)

Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Options

|                   Name                    |  Type   | Required |           Default            |
|-------------------------------------------|---------|----------|------------------------------|
| url                                       | String  | Yes      | -                            |
| driver                                    | String  | Yes      | -                            |
| user                                      | String  | No       | -                            |
| password                                  | String  | No       | -                            |
| query                                     | String  | No       | -                            |
| compatible_mode                           | String  | No       | -                            |
| database                                  | String  | No       | -                            |
| table                                     | String  | No       | -                            |
| primary_keys                              | Array   | No       | -                            |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false                        |
| connection_check_timeout_sec              | Int     | No       | 30                           |
| max_retries                               | Int     | No       | 0                            |
| batch_size                                | Int     | No       | 1000                         |
| is_exactly_once                           | Boolean | No       | false                        |
| generate_sink_sql                         | Boolean | No       | false                        |
| xa_data_source_class_name                 | String  | No       | -                            |
| max_commit_attempts                       | Int     | No       | 3                            |
| transaction_timeout_sec                   | Int     | No       | -1                           |
| auto_commit                               | Boolean | No       | true                         |
| field_ide                                 | String  | No       | -                            |
| properties                                | Map     | No       | -                            |
| common-options                            |         | No       | -                            |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode                            | Enum    | No       | APPEND_DATA                  |
| custom_sql                                | String  | No       | -                            |
| enable_upsert                             | Boolean | No       | true                         |
| use_copy_statement                        | Boolean | No       | false                        |
| create_index                              | Boolean | No       | true                         |

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

The compatible mode of database, required when the database supports multiple compatible modes.

For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'. when using StarRocks, you need set it to `starrocks`.

Postgres 9.5 version or below,please set it to `postgresLow` to support cdc

### database [string]

Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.

This option is mutually exclusive with `query` and has a higher priority.

### table [string]

Use `database` and this `table-name` auto-generate sql and receive upstream input datas write to database.

This option is mutually exclusive with `query` and has a higher priority.

The table parameter can fill in the name of an unwilling table, which will eventually be used as the table name of the creation table, and supports variables (`${table_name}`, `${schema_name}`). Replacement rules: `${schema_name}` will replace the SCHEMA name passed to the target side, and `${table_name}` will replace the name of the table passed to the table at the target side.

mysql sink for example:
1. test_${schema_name}_${table_name}_test
2. sink_sinktable
3. ss_${table_name}

pgsql (Oracle Sqlserver ...) Sink for example:
1. ${schema_name}.${table_name} _test
2. dbo.tt_${table_name} _sink
3. public.sink_table

Tip: If the target database has the concept of SCHEMA, the table parameter must be written as `xxx.xxx`

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

### field_ide [String]

The field "field_ide" is used to identify whether the field needs to be converted to uppercase or lowercase when
synchronizing from the source to the sink. "ORIGINAL" indicates no conversion is needed, "UPPERCASE" indicates
conversion to uppercase, and "LOWERCASE" indicates conversion to lowercase.

### properties

Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details

### schema_save_mode [Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode [Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`CUSTOM_PROCESSING`：User defined processing  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

### custom_sql [String]

When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.

### enable_upsert [boolean]

Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import

### use_copy_statement [boolean]

Use `COPY ${table} FROM STDIN` statement to import data. Only drivers with `getCopyAPI()` method connections are supported.  e.g.: Postgresql driver `org.postgresql.Driver`.

NOTICE: `MAP`, `ARRAY`, `ROW` types are not supported.

### create_index [boolean]

Create the index(contains primary key and any other indexes) or not when auto-create table. You can use this option to improve the performance of jdbc writes when migrating large tables.

Notice: Note that this will sacrifice read performance, so you'll need to manually create indexes after the table migration to improve read performance

## tips

In the case of is_exactly_once = "true", Xa transactions are used. This requires database support, and some databases require some setup :
1 postgres needs to set `max_prepared_transactions > 1` such as `ALTER SYSTEM set max_prepared_transactions to 10`.
2 mysql version need >= `8.0.29` and Non-root users need to grant `XA_RECOVER_ADMIN` permissions. such as `grant XA_RECOVER_ADMIN on test_db.* to 'user1'@'%'`.
3 mysql can try to add `rewriteBatchedStatements=true` parameter in url for better performance.

## appendix

there are some reference value for params above.

|    datasource     |                    driver                    |                                url                                 |             xa_data_source_class_name              |                                                             maven                                                             |
|-------------------|----------------------------------------------|--------------------------------------------------------------------|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| MySQL             | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | com.mysql.cj.jdbc.MysqlXADataSource                | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| PostgreSQL        | org.postgresql.Driver                        | jdbc:postgresql://localhost:5432/postgres                          | org.postgresql.xa.PGXADataSource                   | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| DM                | dm.jdbc.driver.DmDriver                      | jdbc:dm://localhost:5236                                           | dm.jdbc.driver.DmdbXADataSource                    | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                                  |
| Phoenix           | org.apache.phoenix.queryserver.client.Driver | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF | /                                                  | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                                          |
| SQL Server        | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433                                    | com.microsoft.sqlserver.jdbc.SQLServerXADataSource | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                                         |
| Oracle            | oracle.jdbc.OracleDriver                     | jdbc:oracle:thin:@localhost:1521/xepdb1                            | oracle.jdbc.xa.OracleXADataSource                  | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                                            |
| sqlite            | org.sqlite.JDBC                              | jdbc:sqlite:test.db                                                | /                                                  | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                                     |
| GBase8a           | com.gbase.jdbc.Driver                        | jdbc:gbase://e2e_gbase8aDb:5258/test                               | /                                                  | https://cdn.gbase.cn/products/30/p5CiVwXBKQYIUGN8ecHvk/gbase-connector-java-9.5.0.7-build1-bin.jar                   |
| StarRocks         | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| db2               | com.ibm.db2.jcc.DB2Driver                    | jdbc:db2://localhost:50000/testdb                                  | com.ibm.db2.jcc.DB2XADataSource                    | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                                             |
| saphana           | com.sap.db.jdbc.Driver                       | jdbc:sap://localhost:39015                                         | /                                                  | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                                                |
| Doris             | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| teradata          | com.teradata.jdbc.TeraDriver                 | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test              | /                                                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                                                 |
| Redshift          | com.amazon.redshift.jdbc42.Driver            | jdbc:redshift://localhost:5439/testdb                              | com.amazon.redshift.xa.RedshiftXADataSource        | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                                        |
| Snowflake         | net.snowflake.client.jdbc.SnowflakeDriver    | jdbc&#58;snowflake://<account_name>.snowflakecomputing.com         | /                                                  | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                                               |
| Vertica           | com.vertica.jdbc.Driver                      | jdbc:vertica://localhost:5433                                      | /                                                  | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar                               |
| Kingbase          | com.kingbase8.Driver                         | jdbc:kingbase8://localhost:54321/db_test                           | /                                                  | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                                            |
| OceanBase         | com.oceanbase.jdbc.Driver                    | jdbc:oceanbase://localhost:2881                                    | /                                                  | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar                              |
| xugu              | com.xugu.cloudjdbc.Driver                    | jdbc:xugu://localhost:5138                                         | /                                                  | https://repo1.maven.org/maven2/com/xugudb/xugu-jdbc/12.2.0/xugu-jdbc-12.2.0.jar                                               |
| InterSystems IRIS | com.intersystems.jdbc.IRISDriver             | jdbc:IRIS://localhost:1972/%SYS                                    | /                                                  | https://raw.githubusercontent.com/intersystems-community/iris-driver-distribution/main/JDBC/JDK18/intersystems-jdbc-3.8.4.jar |

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

Turn on exact one-time semantics by setting `is_exactly_once`

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

jdbc receive CDC example

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

Add saveMode function

To facilitate the creation of tables when they do not already exist, set the `schema_save_mode`  to `CREATE_SCHEMA_WHEN_NOT_EXIST`.

```
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "123456"
        generate_sink_sql = "true"
        database = "sink_database"
        table = "sink_table"
        primary_keys = ["key1", "key2", ...]
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode="APPEND_DATA"
    }
}
```

Postgresql 9.5 version below support CDC(Change data capture) event

For PostgreSQL versions 9.5 and below, setting `compatible_mode` to `postgresLow` to enable support for PostgreSQL Change Data Capture (CDC) operations.

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

### Multiple table

#### example1

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    
    database = "${database_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

#### example2

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    user = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true

    database = "${schema_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
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

