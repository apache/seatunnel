# JDBC

> JDBC 数据接收器

## 描述

通过jdbc写入数据。支持批处理模式和流式模式，支持并发写入，支持一次语义(使用XA事务保证)

## 使用依赖

### 用于Spark/Flink引擎

> 1. 需要确保jdbc驱动jar包已经放在目录`${SEATUNNEL_HOME}/plugins/`下。

### 适用于 SeaTunnel Zeta 引擎

> 1. 需要确保jdbc驱动jar包已经放到`${SEATUNNEL_HOME}/lib/`目录下。

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)

使用 `Xa transactions` 来确保 `exactly-once`。所以对于支持 `Xa transactions` 的数据库只支持 `exactly-once`
。您可以设置 `is_exactly_once=true` 来启用它。

- [x] [cdc](../../concept/connector-v2-features.md)

## Options

| 名称                                        | 类型      | 是否必须 | 默认值                          |
|-------------------------------------------|---------|------|------------------------------|
| url                                       | String  | 是    | -                            |
| driver                                    | String  | 是    | -                            |
| user                                      | String  | 否    | -                            |
| password                                  | String  | 否    | -                            |
| query                                     | String  | 否    | -                            |
| compatible_mode                           | String  | 否    | -                            |
| database                                  | String  | 否    | -                            |
| table                                     | String  | 否    | -                            |
| primary_keys                              | Array   | 否    | -                            |
| support_upsert_by_query_primary_key_exist | Boolean | 否    | false                        |
| connection_check_timeout_sec              | Int     | 否    | 30                           |
| max_retries                               | Int     | 否    | 0                            |
| batch_size                                | Int     | 否    | 1000                         |
| is_exactly_once                           | Boolean | 否    | false                        |
| generate_sink_sql                         | Boolean | 否    | false                        |
| xa_data_source_class_name                 | String  | 否    | -                            |
| max_commit_attempts                       | Int     | 否    | 3                            |
| transaction_timeout_sec                   | Int     | 否    | -1                           |
| auto_commit                               | Boolean | 否    | true                         |
| field_ide                                 | String  | 否    | -                            |
| properties                                | Map     | 否    | -                            |
| common-options                            |         | 否    | -                            |
| schema_save_mode                          | Enum    | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode                            | Enum    | 否    | APPEND_DATA                  |
| custom_sql                                | String  | 否    | -                            |
| enable_upsert                             | Boolean | 否    | true                         |
| use_copy_statement                        | Boolean | 否    | false                        |

### driver [string]

用于连接远程数据源的jdbc类名，如果使用MySQL，则值为`com.mysql.cj.jdbc.Driver`

### user [string]

用户名

### password [string]

密码

### url [string]

JDBC 连接的 URL。参考案例：`jdbc:postgresql://localhost/test`

### query [string]

使用sql将上游输入数据写入数据库。如 `INSERT ...`

### compatible_mode [string]

数据库的兼容模式，当数据库支持多种兼容模式时需要。例如，使用OceanBase数据库时，需要将其设置为'mysql'或'oracle'。

Postgres 9.5或以下版本，请设置为`postgresLow`以支持cdc

### database [string]

使用此 `database` 和 `table-name` 自动生成sql并接收上游输入数据写入数据库

该选项与 `query` 互斥，并且具有更高的优先级

### table [string]

使用 `database` 和这个 `table-name` 自动生成sql并接收上游输入数据写入数据库

该选项与 `query` 互斥，并且具有更高的优先级。

table参数可以填写不愿意的表的名称，最终会作为创建表的表名，支持变量（`${table_name}`、`${schema_name}`
）。替换规则：`${schema_name}`将替换传递到目标端的SCHEMA名称，`${table_name}`将替换传递到目标端表的表名称。

mysql 接收器示例:

1. test_${schema_name}_${table_name}_test
2. sink_sinktable
3. ss_${table_name}

pgsql (Oracle Sqlserver ...) 接收器示例:

1. ${schema_name}.${table_name} _test
2. dbo.tt_${table_name} _sink
3. public.sink_table

Tip: 如果目标数据库有SCHEMA的概念，则表参数必须写成`xxx.xxx`

### primary_keys [array]

该选项用于支持自动生成sql时的insert、delete、update等操作

### support_upsert_by_query_primary_key_exist [boolean]

根据查询主键是否存在选择使用INSERT sql、UPDATE sql来处理更新事件(INSERT、UPDATE_AFTER)。
仅当数据库不支持 upsert 语法时才使用此配置。
**注意**：该方法性能较低

### connection_check_timeout_sec [int]

等待用于验证连接的数据库操作完成的时间（以秒为单位）。

### max_retries[int]

重试提交失败的次数（executeBatch）

### batch_size[int]

对于批量写入，当缓冲的记录数达到 `batch_size` 数量或者时间达到 `checkpoint.interval` 时，数据将被刷新到数据库中

### is_exactly_once[boolean]

是否启用一次语义，这将使用 Xa 事务。如果打开，您需要设置 `xa_data_source_class_name`

### generate_sink_sql[boolean]

根据要写入的数据库表生成sql语句

### xa_data_source_class_name[string]

数据库Driver的XA数据源类名，例如mysql为`com.mysql.cj.jdbc.MysqlXADataSource`，其他数据源请参考附录

### max_commit_attempts[int]

事务提交失败的重试次数

### transaction_timeout_sec[int]

交易开启后的超时时间，默认为-1（永不超时）。请注意，设置超时可能会影响恰好一次语义

### auto_commit [boolean]

默认启用自动事务提交

### field_ide [String]

字段`field_ide`用于标识从源到宿同步时该字段是否需要转换为大写或小写。`ORIGINAL` 表示不需要转换，`UPPERCASE`
表示转换为大写，`LOWERCASE` 表示转换为小写。

### properties

附加连接配置参数，当属性和URL具有相同参数时，优先级由<br/>
驱动程序的具体实现确定。例如，在 MySQL 中，属性优先于 URL。

### common options

Sink插件常用参数，请参考 [Sink常用选项](../../transform-v2/common-options.md) 了解详情

### schema_save_mode [Enum]

在开启同步任务之前，针对目标面现有的表面结构选择不同的处理方案。选项介绍：
`RECREATE_SCHEMA` ：表不存在时创建，保存表时删除并重建<br/>
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：表不存在时创建，保存表时跳过<br/>
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：会出错表不存在时报告<br/>

### data_save_mode [Enum]

在开启同步任务之前，针对目标端已有的数据选择不同的处理方案。
选项介绍：
`DROP_DATA`：保留数据库结构，删除数据<br/>
`APPEND_DATA`：保留数据库结构，保留数据<br/>
`CUSTOM_PROCESSING`：用户自定义处理<br/>
`ERROR_WHEN_DATA_EXISTS`：有数据时报错<br/>

### custom_sql [String]

当`data_save_mode`选择`CUSTOM_PROCESSING`时，需要填写`CUSTOM_SQL`参数。该参数通常填写一条可以执行的SQL。SQL将在同步任务之前执行。

### enable_upsert [boolean]

启用通过主键更新插入，如果任务没有key重复数据，设置该参数为 false 可以加快数据导入速度

### use_copy_statement [boolean]

使用 `COPY ${table} FROM STDIN` 语句导入数据。仅支持具有 `getCopyAPI()` 方法连接的驱动程序。例如：Postgresql
驱动程序 `org.postgresql.Driver`。

注意：不支持 `MAP`、`ARRAY`、`ROW`类型。

## tips

在 is_exactly_once = "true" 的情况下，使用 Xa 事务。这需要数据库支持，有些数据库需要一些设置：<br/>
1 postgres 需要设置 `max_prepared_transactions > 1` 例如 `ALTER SYSTEM set max_prepared_transactions to 10` <br/>
2 mysql 版本需要 >= `8.0.29` 并且非 root 用户需要授予 `XA_RECOVER_ADMIN` 权限。例如:将 test_db.* 上的 XA_RECOVER_ADMIN
授予 `'user1'@'%'`<br/>
3 mysql可以尝试在url中添加 `rewriteBatchedStatements=true` 参数以获得更好的性能<br/>

## 附录

上面的params有些许参考价值

| 数据源        | driver                                       | url                                                                | xa_data_source_class_name                          | maven                                                                                                       |
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
| Kingbase   | com.kingbase8.Driver                         | jdbc:kingbase8://localhost:54321/db_test                           | /                                                  | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                          |
| OceanBase  | com.oceanbase.jdbc.Driver                    | jdbc:oceanbase://localhost:2881                                    | /                                                  | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.3/oceanbase-client-2.4.3.jar              |

## 示例

简单示例

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

CDC(Change data capture) 事件

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

增加 SaveMode 功能

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
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode="APPEND_DATA"
    }
}
```

Postgresql 9.5以下版本支持CDC(Change data capture)事件

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

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加控制台接收器连接器

### 2.3.0-beta 2022-10-20

- [BugFix] Fix JDBC split exception ([2904](https://github.com/apache/seatunnel/pull/2904))
- [Feature] Support Phoenix JDBC Sink ([2499](https://github.com/apache/seatunnel/pull/2499))
- [Feature] Support SQL Server JDBC Sink ([2646](https://github.com/apache/seatunnel/pull/2646))
- [Feature] Support Oracle JDBC Sink ([2550](https://github.com/apache/seatunnel/pull/2550))
- [Feature] Support StarRocks JDBC Sink ([3060](https://github.com/apache/seatunnel/pull/3060))
- [Feature] Support DB2 JDBC Sink ([2410](https://github.com/apache/seatunnel/pull/2410))

### next version

- [Feature] Support CDC write DELETE/UPDATE/INSERT events ([3378](https://github.com/apache/seatunnel/issues/3378))
- [Feature] Support Teradata JDBC Sink ([3362](https://github.com/apache/seatunnel/pull/3362))
- [Feature] Support Sqlite JDBC Sink ([3089](https://github.com/apache/seatunnel/pull/3089))
- [Feature] Support CDC write DELETE/UPDATE/INSERT events ([3378](https://github.com/apache/seatunnel/issues/3378))
- [Feature] Support Doris JDBC Sink
- [Feature] Support Redshift JDBC Sink([#3615](https://github.com/apache/seatunnel/pull/3615))
- [Improve] Add config item enable upsert by query([#3708](https://github.com/apache/seatunnel/pull/3708))
- [Improve] Add database field to sink config([#4199](https://github.com/apache/seatunnel/pull/4199))
- [Improve] Add Vertica connector([#4303](https://github.com/apache/seatunnel/pull/4303))

