# JDBC

> JDBC 源连接器

## 描述

通过 JDBC 读取外部数据源数据。

:::tip

警告：为了符合许可证要求，您需要自行提供数据库驱动程序，并将其复制到 `$SEATUNNEL_HOME/lib/` 目录下才能使用。

例如，如果您使用 MySQL，需要下载并复制 `mysql-connector-java-xxx.jar` 到 `$SEATUNNEL_HOME/lib/`。对于 Spark/Flink，您还需要将其复制到 `$SPARK_HOME/jars/` 或 `$FLINK_HOME/lib/`。

:::

## 使用依赖

### Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)

支持查询 SQL 并可以实现投影效果。

- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户自定义分片](../../concept/connector-v2-features.md)
- [x] [支持多表读取](../../concept/connector-v2-features.md)

## 配置项

| 名称                                       | 类型    | 是否必须 | 默认值          | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------|---------|----------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String  | 是       | -               | JDBC 连接的 URL。参考示例：jdbc:postgresql://localhost/test                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| driver                                     | String  | 是       | -               | 用于连接远程数据源的 jdbc 类名，如果使用 MySQL，值为 `com.mysql.cj.jdbc.Driver`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| user                                       | String  | 否       | -               | 用户名                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| password                                   | String  | 否       | -               | 密码                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| query                                      | String  | 否       | -               | 查询语句                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| compatible_mode                            | String  | 否       | -               | 数据库的兼容模式，当数据库支持多种兼容模式时需要设置。<br/> 例如，使用 OceanBase 数据库时，需要设置为 'mysql' 或 'oracle'。<br/> 使用 starrocks 时，需要设置为 `starrocks`                                                                                                                                                                                                                                                                                                                                                                                             |
| dialect                                    | String  | 否       | -               | 指定的方言，如果不存在，仍然根据 url 获取，优先级高于 url。<br/> 例如，使用 starrocks 时，需要设置为 `starrocks`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| connection_check_timeout_sec               | Int     | 否       | 30              | 用于验证连接的数据库操作等待时间（秒）。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| partition_column                           | String  | 否       | -               | 用于分片数据的列名。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| partition_upper_bound                      | Long    | 否       | -               | 用于扫描的 partition_column 最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_lower_bound                      | Long    | 否       | -               | 用于扫描的 partition_column 最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_num                              | Int     | 否       | job parallelism | 不推荐使用，正确的方式是通过 `split.size` 控制分片数量<br/> **注意：** 此参数仅在使用 `query` 参数时生效。使用 `table_path` 参数时不生效。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| decimal_type_narrowing                     | Boolean | 否       | true            | Decimal 类型缩小，如果为 true，decimal 类型将在不损失精度的情况下缩小为 int 或 long 类型。目前仅支持 Oracle。请参考下面的 `decimal_type_narrowing`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| handle_blob_as_string                      | Boolean | 否       | false           | 如果为 true，BLOB 类型将被转换为 STRING 类型。**仅支持 Oracle 数据库**。这对于处理 Oracle 中超过默认大小限制的大型 BLOB 字段很有用。当将 Oracle 的 BLOB 字段传输到 Doris 等系统时，将其设置为 true 可以使数据传输更高效。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| use_select_count                           | Boolean | 否       | false           | 在动态分片阶段使用 select count 而不是其他方法进行表计数。目前仅适用于 jdbc-oracle。在这种情况下，当使用分析表的 sql 更新统计信息较慢时，直接使用 select count                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| skip_analyze                               | Boolean | 否       | false           | 在动态分片阶段跳过表计数分析。目前仅适用于 jdbc-oracle。在这种情况下，您可以定期调度分析表 sql 来更新相关表统计信息，或者您的表数据不经常更改                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| fetch_size                                 | Int     | 否       | 0               | 对于返回大量对象的查询，您可以配置查询中使用的行获取大小，通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| properties                                 | Map     | 否       | -               | 额外的连接配置参数，当 properties 和 URL 有相同的参数时，优先级由驱动程序的特定实现决定。<br/> 例如，在 MySQL 中，properties 优先于 URL。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| table_path                                 | String  | 否       | -               | 表的完整路径，您可以使用此配置代替 `query`。<br/>示例：<br/>`- mysql: "testdb.table1" `<br/>`- oracle: "test_schema.table1" `<br/>`- sqlserver: "testdb.test_schema.table1"` <br/>`- postgresql: "testdb.test_schema.table1"`  <br/>`- iris: "test_schema.table1"` <br/>您还可以使用正则表达式来匹配多个表。例如：<br/>`- "testdb.table\\d+"` 将匹配 "testdb" 数据库中所有以 "table" 开头后跟数字的表。 |
| table_list                                 | Array   | 否       | -               | 要读取的表列表，您可以使用此配置代替 `table_path`。列表中的每个表都应遵循与 `table_path` 相同的格式。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| where_condition                            | String  | 否       | -               | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.size                                 | Int     | 否       | 8096            | 一个分片中的行数，读取表时将捕获的表分成多个分片。**注意**：此参数仅在使用 `table_path` 参数时生效。使用 `query` 参数时不生效。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.even-distribution.factor.lower-bound | Double  | 否       | 0.05            | 不推荐使用。<br/> 分片键分布因子的下限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），表分片将针对均匀分布进行优化。否则，如果分布因子较小，当估计的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 0.05。  |
| split.even-distribution.factor.upper-bound | Double  | 否       | 100             | 不推荐使用。<br/> 分片键分布因子的上限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），表分片将针对均匀分布进行优化。否则，如果分布因子较大，当估计的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 100.0。 |
| split.sample-sharding.threshold            | Int     | 否       | 1000            | 此配置指定触发采样分片策略的估计分片数阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且估计的分片数（计算为近似行数 / 分片大小）超过此阈值时，将使用采样分片策略。这可以帮助更有效地处理大型数据集。默认值为 1000 个分片。                                                                                                                 |
| split.inverse-sampling.rate                | Int     | 否       | 1000            | 采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则表示在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数。在处理非常大的数据集时特别有用，此时首选较低的采样率。默认值为 1000。                                                                                                                                                                                            |
| common-options                             |         | 否       | -               | 源插件通用参数，请参阅 [Source Common Options](../source-common-options.md) 了解详情。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| split.string_split_mode                    | String  | 否       | sample          | 支持不同的字符串分割算法。默认情况下，使用 `sample` 通过采样字符串值来确定分割。您可以切换到 `charset_based` 以启用基于字符集的字符串分割算法。当设置为 `charset_based` 时，算法假设 partition_column 的字符在 ASCII 范围 32-126 内，这涵盖了大多数基于字符的分割场景。                                                                                                                                                                                                                                                    |
| split.string_split_mode_collate            | String  | 否       | -               | 当 string_split_mode 设置为 `charset_based` 且表有特殊排序规则时，指定要使用的排序规则。如果未指定，将使用数据库的默认排序规则。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

## 表匹配

JDBC 源连接器支持两种指定表的方式：

1. **精确表路径**：使用 `table_path` 指定单个表的完整路径。
   ```hocon
   table_path = "testdb.table1"
   ```

2. **正则表达式**：使用带有正则表达式模式的 `table_path` 来匹配多个表。
   ```hocon
   table_path = "testdb.table\\d+"  # 匹配 table1、table2、table3 等
   ```

3. **表列表**：使用 `table_list` 显式指定多个表。
   ```hocon
   table_list = [
     "testdb.table1",
     "testdb.table2",
     "testdb.table3"
   ]
   ```

### 正则表达式支持

JDBC 源连接器支持使用正则表达式进行表匹配。当您想要读取遵循特定命名模式的多个表时，这很有用。

示例：
- `testdb.table\\d+` - 匹配所有以 "table" 开头后跟数字的表
- `testdb.user_.*` - 匹配所有以 "user_" 开头的表
- `testdb.*_2023` - 匹配所有以 "_2023" 结尾的表

注意：正则表达式模式应该匹配完整的表路径（包括数据库/模式名称）。

### 多表同步

当使用正则表达式或表列表时，连接器将从所有匹配的表读取数据。每个表将独立处理，数据将在输出中组合。

多表同步的配置示例：
```hocon
Jdbc {
    url = "jdbc:mysql://localhost/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    
    # 使用正则表达式
    table_path = "testdb.table\\d+"
    
    # 或使用表列表
    # table_list = ["testdb.table1", "testdb.table2", "testdb.table3"]
}
```

### decimal_type_narrowing

Decimal 类型缩小，如果为 true，decimal 类型将在不损失精度的情况下缩小为 int 或 long 类型。目前仅支持 Oracle。

例如：

decimal_type_narrowing = true

| Oracle        | SeaTunnel |
|---------------|-----------|
| NUMBER(1, 0)  | Boolean   |
| NUMBER(6, 0)  | INT       |
| NUMBER(10, 0) | BIGINT    |

decimal_type_narrowing = false

| Oracle        | SeaTunnel      |
|---------------|----------------|
| NUMBER(1, 0)  | Decimal(1, 0)  |
| NUMBER(6, 0)  | Decimal(6, 0)  |
| NUMBER(10, 0) | Decimal(10, 0) |

### dialect [string]

指定的方言，如果不存在，仍然根据 url 获取，优先级高于 url。例如，使用 starrocks 时，需要设置为 `starrocks`。同样，使用 mysql 时，需要将其值设置为 `mysql`。

#### 方言列表

|           | Dialect Name |          |
|-----------|--------------|----------|
| Greenplum | DB2          | Dameng   |
| Gbase8a   | HIVE         | KingBase |
| MySQL     | StarRocks    | Oracle   |
| Phoenix   | Postgres     | Redshift |
| SapHana   | Snowflake    | Sqlite   |
| SqlServer | Tablestore   | Teradata |
| Vertica   | OceanBase    | XUGU     |
| IRIS      | Inceptor     | Highgo   |

## 并行读取

JDBC 源连接器支持并行读取表中的数据。SeaTunnel 将使用某些规则来分割表中的数据，这些数据将交给读取器进行读取。读取器的数量由 `parallelism` 选项决定。

**分片键规则：**

1. 如果 `partition_column` 不为 null，它将用于计算分片。该列必须在**支持的分片数据类型**中。
2. 如果 `partition_column` 为 null，seatunnel 将从表中读取模式并获取主键和唯一索引。如果主键和唯一索引中有多个列，将使用**支持的分片数据类型**中的第一列来分割数据。例如，表有主键(nn guid, name varchar)，因为 `guid` 不在**支持的分片数据类型**中，所以将使用列 `name` 来分割数据。

**支持的分片数据类型：**
* String
* Number(int, bigint, decimal, ...)
* Date

## 提示

> 如果表无法分片（例如，表没有主键或唯一索引，且未设置 `partition_column`），它将以单并发运行。
>
> 使用 `table_path` 代替 `query` 进行单表读取。如果需要读取多个表，请使用 `table_list`。

## 附录

上面参数的一些参考值。

| 数据源        | driver                                              | url                                                                    | maven                                                                                                                         |
|-------------------|-----------------------------------------------------|------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| mysql             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| postgresql        | org.postgresql.Driver                               | jdbc:postgresql://localhost:5432/postgres                              | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| dm                | dm.jdbc.driver.DmDriver                             | jdbc:dm://localhost:5236                                               | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                                  |
| phoenix           | org.apache.phoenix.queryserver.client.Driver        | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF     | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                                          |
| sqlserver         | com.microsoft.sqlserver.jdbc.SQLServerDriver        | jdbc:sqlserver://localhost:1433                                        | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                                         |
| oracle            | oracle.jdbc.OracleDriver                            | jdbc:oracle:thin:@localhost:1521/xepdb1                                | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                                            |
| sqlite            | org.sqlite.JDBC                                     | jdbc:sqlite:test.db                                                    | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                                     |
| gbase8a           | com.gbase.jdbc.Driver                               | jdbc:gbase://e2e_gbase8aDb:5258/test                                   | https://cdn.gbase.cn/products/30/p5CiVwXBKQYIUGN8ecHvk/gbase-connector-java-9.5.0.7-build1-bin.jar                            |
| starrocks         | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| db2               | com.ibm.db2.jcc.DB2Driver                           | jdbc:db2://localhost:50000/testdb                                      | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                                             |
| tablestore        | com.alicloud.openservices.tablestore.jdbc.OTSDriver | "jdbc:ots:http s://myinstance.cn-hangzhou.ots.aliyuncs.com/myinstance" | https://mvnrepository.com/artifact/com.aliyun.openservices/tablestore-jdbc                                                    |
| saphana           | com.sap.db.jdbc.Driver                              | jdbc:sap://localhost:39015                                             | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                                                |
| doris             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| teradata          | com.teradata.jdbc.TeraDriver                        | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                                                 |
| Snowflake         | net.snowflake.client.jdbc.SnowflakeDriver           | jdbc&#58;snowflake://<account_name>.snowflakecomputing.com             | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                                               |
| Redshift          | com.amazon.redshift.jdbc42.Driver                   | jdbc:redshift://localhost:5439/testdb?defaultRowFetchSize=1000         | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                                        |
| Vertica           | com.vertica.jdbc.Driver                             | jdbc:vertica://localhost:5433                                          | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar                               |
| Kingbase          | com.kingbase8.Driver                                | jdbc:kingbase8://localhost:54321/db_test                               | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                                            |
| OceanBase         | com.oceanbase.jdbc.Driver                           | jdbc:oceanbase://localhost:2881                                        | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar                              |
| Hive              | org.apache.hive.jdbc.HiveDriver                     | jdbc:hive2://localhost:10000                                           | https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar                                 |
| xugu              | com.xugu.cloudjdbc.Driver                           | jdbc:xugu://localhost:5138                                             | https://repo1.maven.org/maven2/com/xugudb/xugu-jdbc/12.2.0/xugu-jdbc-12.2.0.jar                                               |
| InterSystems IRIS | com.intersystems.jdbc.IRISDriver                    | jdbc:IRIS://localhost:1972/%SYS                                        | https://raw.githubusercontent.com/intersystems-community/iris-driver-distribution/main/JDBC/JDK18/intersystems-jdbc-3.8.4.jar |
| opengauss         | org.opengauss.Driver                                | jdbc:opengauss://localhost:5432/postgres                               | https://repo1.maven.org/maven2/org/opengauss/opengauss-jdbc/5.1.0-og/opengauss-jdbc-5.1.0-og.jar                              |
| Highgo            | com.highgo.jdbc.Driver                              | jdbc:highgo://localhost:5866/highgo                                    | https://repo1.maven.org/maven2/com/highgo/HgdbJdbc/6.2.3/HgdbJdbc-6.2.3.jar                                                   |

## 示例

### 简单示例

#### 示例 1

```hocon
Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"
    query = "select * from type_bin"
}
```

#### 示例 2 在动态分片阶段使用 select count(*) 代替分析表来计数表行数

```hocon
Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"
    use_select_count = true 
    query = "select * from type_bin"
}
```

#### 示例 3 使用 select NUM_ROWS from all_tables 来获取表行数但跳过分析表

```hocon
Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"
    skip_analyze = true
    query = "select * from type_bin"
}
``` 