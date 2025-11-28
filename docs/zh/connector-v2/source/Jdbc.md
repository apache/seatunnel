import ChangeLog from '../changelog/connector-jdbc.md';

# JDBC

> JDBC 源连接器

## 描述

通过 JDBC 读取外部数据源数据。

:::tip

警告：为了符合许可证要求，您必须自己提供数据库驱动程序，复制到 `$SEATUNNEL_HOME/lib/` 目录以使其工作。

例如，如果您使用 MySQL，应下载并复制 `mysql-connector-java-xxx.jar` 到 `$SEATUNNEL_HOME/lib/`。对于 Spark/Flink，您还应将其复制到 `$SPARK_HOME/jars/` 或 `$FLINK_HOME/lib/`。

:::

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 关键特性

- [x] [批](../../concept/connector-v2-features.md)
- [ ] [流](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)

支持查询 SQL 并可以实现投影效果。

- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户自定义split](../../concept/connector-v2-features.md)
- [x] [支持多表读取](../../concept/connector-v2-features.md)

## 选项

| 参数名                                       | 类型    | 必须 | 默认值   | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------|---------|------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String  | 是   | -       | JDBC 连接的 URL。参考示例：jdbc:postgresql://localhost/test                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| driver                                     | String  | 是   | -       | 用于连接到远程数据源的 jdbc 类名，如果您使用 MySQL，值为 `com.mysql.cj.jdbc.Driver`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| username                                       | String  | 否   | -       | 用户名                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| password                                   | String  | 否   | -       | 密码                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| connection_check_timeout_sec               | Int     | 否   | 30      | 验证数据库连接是否可用的等待时间（秒）                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| partition_column                           | String  | 否   | -       | 用于划分分区的列名                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| partition_upper_bound                      | BigDecimal | 否   | -       | 读取时 `partition_column` 的最大值，如果未配置，SeaTunnel 将从数据库中获取最大值                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| partition_lower_bound                      | BigDecimal | 否   | -       | 读取时 `partition_column` 的最小值，如果未配置，SeaTunnel 将从数据库中获取最小值                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| partition_num                              | Int     | 否   | -       | 要划分为多少个分区，仅支持数值类型，默认值为任务并行度                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| fetch_size                                 | Int     | 否   | 0       | 查询返回大量对象时，可以配置查询中使用的行抓取大小，以通过减少满足选择条件所需的数据库访问次数来提高性能。0 表示使用 JDBC 默认值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| query                                      | String  | 否   | -       | 用户自定义查询语句                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| use_select_count                           | Boolean | 否   | false   | 在动态块分割阶段使用 `select count(*)` 代替分析表来计算表行数                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| skip_analyze                               | Boolean | 否   | false   | 使用 `select NUM_ROWS from all_tables` 获取表行数但跳过分析表                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| use_regex                                  | Boolean | 否   | false   | 是否使用正则表达式匹配表路径                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| table_path                                 | String  | 否   | -       | 表全路径，例如 `db1.schema1.table1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| table_list                                 | List<JdbcSourceTableConfig> | 否 | -   | 表配置列表，支持多表读取                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| split.size                                 | Int     | 否   | 8096    | 表快照的分片大小（行数），捕获的表在读取时将被拆分为多个分片                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| split.even-distribution.factor.upper-bound | Double  | 否   | 100     | 不建议使用。块键分布因子上限。此因子用于确定表数据是否均匀分布。如果计算得出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），表块将被优化以实现均匀分布。否则，如果分布因子大于此上限，表将被视为分布不均，如果估计的分片数量超过 `sample-sharding.threshold` 指定的值，将使用基于采样的分片策略。默认值为 100.0。                                                                                                                                            |
| split.even-distribution.factor.lower-bound | Double  | 否   | 0.05    | 不建议使用。块键分布因子下限。此因子用于确定表数据是否均匀分布。当分布因子小于此下限时，表将被视为分布极不均匀，如果估计分片数量超过 `sample-sharding.threshold` 指定的值，将使用基于采样的分片策略。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| split.sample-sharding.threshold            | Int     | 否   | 1000    | 此配置指定触发采样分片策略的预计分片数量阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，且估计的分片数量（计算为近似行数 / 块大小）超过此阈值时，将使用采样分片策略。这有助于更高效地处理大型数据集。默认值为 1000 个分片。                                                                                                                           |
| split.inverse-sampling.rate                | Int     | 否   | 1000    | 采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则表示在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理非常大的数据集时特别有用，此时通常倾向于使用较低的采样率。默认值为 1000。                                                                                                                             |
| split.string_split_mode                    | String  | 否   | sample  | 支持不同的字符串分割算法。默认情况下，使用 `sample` 通过对字符串值进行采样来确定分割。您可以切换为 `charset_based` 以启用基于字符集的字符串分割算法。设置为 `charset_based` 时，该算法假设 partition_column 的字符在 ASCII 范围 32-126 内，这涵盖了大多数基于字符的分割场景。                                                                                                                                                                                                                                             |
| split.string_split_mode_collate            | String  | 否   | -       | 指定当 `string_split_mode` 设置为 `charset_based` 且表具有特殊排序规则时要使用的排序规则。如果未指定，将使用数据库的默认排序规则。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| server_time_zone                           | String  | 否   | -       | 数据库服务器的会话时区，例如 `"Asia/Shanghai"` 或 `"UTC"`。它控制在使用 JDBC 驱动程序（如 MySQL）时 `TIMESTAMP` 列如何在数据库和 JVM 之间转换。如果未设置，驱动程序通常会回退到 JVM 默认时区或其自身默认值，当数据库服务器在不同时区运行时，这可能导致小时偏差。                                                                                                                                                                                                                                                                                                                                 |
| common-options                             |         | 否   | -       | 源插件通用参数，请参考 [源通用选项](../source-common-options.md) 详见。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

### dialect [string]

指定方言，如果不存在，则仍然根据 url 获取，优先级高于 url。例如，当使用 StarRocks 时，需要将其设置为 `starrocks`。同样地，当使用 MySQL 时，需要将其设置为 `mysql`。

如果某个方言不被 SeaTunnel 支持，它将使用默认方言 `GenericDialect`。只需确保您提供的驱动程序支持要连接的数据库即可。

### server_time_zone [string]

数据库服务器的会话时区，例如 `"Asia/Shanghai"` 或 `"UTC"`。它控制在使用 JDBC 驱动程序（如 MySQL）时 `TIMESTAMP` 列如何在数据库和 JVM 之间转换。如果未设置，驱动程序通常会回退到 JVM 默认时区或其自身默认值，当数据库服务器在不同时区运行时，这可能导致小时偏差。

## 并行读取

JDBC 源连接器支持并行读取表中的数据。SeaTunnel 将使用一定的规则对表中的数据进行拆分，然后交由多个 Reader 进行读取。Reader 的数量由 `parallelism` 参数确定。

**分片键规则：**

1. 如果配置了 `partition_column`，则以它作为切分键。切分键必须是**支持分片的数据类型**。
2. 如果未配置 `partition_column`，SeaTunnel 将从表中读取 schema，获取主键和唯一索引。如果主键或唯一索引中包含多个列，则选择第一个属于**支持分片数据类型**的列作为切分键。例如，表的主键为 (guid, name)，其中 `guid` 不是支持分片的数据类型，则会选择 `name` 作为切分列。

**支持的分片数据类型：**
* String
* 数值类型（int、bigint、decimal 等）
* Date

## 提示

> 如果表无法分割（例如，表没有主键或唯一索引，且未设置 `partition_column`），它将以单并发运行。
>
> 使用 `table_path` 替换 `query` 进行单表读取。如果需要读取多个表，请使用 `table_list`。

## 附录

以上参数有一些参考值。

| 数据源        | 驱动                                              | URL                                                                    | Maven                                                                                                                         |
|-------------|---------------------------------------------------|--------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| mysql             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| postgresql        | org.postgresql.Driver                               | jdbc:postgresql://localhost:5432/postgres                              | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| dm                | dm.jdbc.driver.DmDriver                             | jdbc:dm://localhost:5236                                               | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                                  |
| oracle            | oracle.jdbc.OracleDriver                            | jdbc:oracle:thin:@localhost:1521/xepdb1                                | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                                            |
| sqlserver         | com.microsoft.sqlserver.jdbc.SQLServerDriver        | jdbc:sqlserver://localhost:1433                                        | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                                         |
| starrocks         | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| kingbase          | com.kingbase8.Driver                                | jdbc:kingbase8://localhost:54321/db_test                               | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                                            |
| oceanbase         | com.oceanbase.jdbc.Driver                           | jdbc:oceanbase://localhost:2881                                        | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar                              |
| hive              | org.apache.hive.jdbc.HiveDriver                     | jdbc:hive2://localhost:10000                                           | https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar                                 |

## 示例

### 简单

#### 情况 1

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

#### 情况 2 在动态块分割阶段使用 select count(*) 代替分析表来计算表行数

```
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

#### 情况 3 使用 select NUM_ROWS from all_tables 获取表行数但跳过分析表

```
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

## 变更日志

<ChangeLog />

