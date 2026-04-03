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

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

支持查询 SQL 并可以实现投影效果。

- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                                       | 类型    | 必须 | 默认值   | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------|---------|------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String  | 是   | -       | JDBC 连接的 URL。参考示例：jdbc:postgresql://localhost/test                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| driver                                     | String  | 是   | -       | 用于连接到远程数据源的 jdbc 类名，如果您使用 MySQL，值为 `com.mysql.cj.jdbc.Driver`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| username                                       | String  | 否   | -       | 用户名                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| password                                   | String  | 否   | -       | 密码                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| query                                      | String  | 否   | -       | 查询语句                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| compatible_mode                            | String  | 否   | -       | 数据库的兼容模式，当数据库支持多种兼容模式时需要。<br/> 例如，使用 OceanBase 数据库时，需要将其设置为 'mysql' 或 'oracle'。<br/> 使用 starrocks 时，需要将其设置为 `starrocks`                                                                                                                                                                                                                                                                                                                                                                                                             |
| dialect                                    | String  | 否   | -       | 指定的方言，如果不存在，仍然根据 url 获取，优先级高于 url。<br/> 例如，使用 starrocks 时，需要将其设置为 `starrocks`                                                                                                                                                                                                                                                                                                                                                                                                                               |
| connection_check_timeout_sec               | Int     | 否   | 30      | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| partition_column                           | String  | 否   | -       | 用于分割数据的列名。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| partition_upper_bound                      | Long    | 否   | -       | partition_column 的最大值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最大值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_lower_bound                      | Long    | 否   | -       | partition_column 的最小值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最小值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_num                              | Int     | 否   | job parallelism | 不建议使用，正确的方法是通过 `split.size` 控制分割数量<br/> **注意：** 此参数仅在使用 `query` 参数时生效。使用 `table_path` 参数时不生效。                                                                                                                                                                                                                                                                                                                                                                                              |
| decimal_type_narrowing                     | Boolean | 否   | true    | 十进制类型缩小，如果为 true，十进制类型将缩小为 int 或 long 类型（如果没有精度损失）。目前仅支持 Oracle。请参考下面的 `decimal_type_narrowing`                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| int_type_narrowing                         | Boolean | 否   | true    | Int 类型缩小，如果为 true，tinyint(1) 类型将缩小为布尔类型（如果没有精度损失）。目前支持 MySQL。请参考下面的 `int_type_narrowing`                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| handle_blob_as_string                      | Boolean | 否   | false   | 如果为 true，BLOB 类型将转换为 STRING 类型。**仅支持 Oracle 数据库**。这对于处理超过默认大小限制的 Oracle 中的大 BLOB 字段很有用。将 Oracle 的 BLOB 字段传输到 Doris 等系统时，将其设置为 true 可以使数据传输更高效。                                                                                                                                                                                                                                                                                                                                                                                                                             |
| use_select_count                           | Boolean | 否   | false   | 在动态块分割阶段使用 select count 来获取表计数，而不是其他方法。这目前仅适用于 jdbc-oracle。在这种情况下，当使用 sql 从分析表更新统计信息更快时，直接使用 select count                                                                                                                                                                                                                                                                                                                                                                                                     |
| skip_analyze                               | Boolean | 否   | false   | 在动态块分割阶段跳过表计数分析。这目前仅适用于 jdbc-oracle。在这种情况下，您定期安排分析表 sql 来更新相关表统计信息，或您的表数据不经常更改                                                                                                                                                                                                                                                                                                                                                                                                    |
| use_regex                                  | Boolean | 否   | false   | 控制 table_path 的正则表达式匹配。设置为 `true` 时，table_path 将被视为正则表达式模式。设置为 `false` 或未指定时，table_path 将被视为精确路径（无正则表达式匹配）。 |
| fetch_size                                 | Int     | 否   | 0       | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，以通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。                                                                                                                                                                                                                                                                                                                                                                                                               |
| properties                                 | Map     | 否   | -       | 其他连接配置参数，当 properties 和 URL 具有相同参数时，优先级由<br/>驱动程序的具体实现确定。例如，在 MySQL 中，properties 优先于 URL。                                                                                                                                                                                                                                                                                                                                                                                                     |
| table_path                                 | String  | 否   | -       | 表的完整路径，您可以使用此配置代替 `query`。<br/>示例：<br/>`- mysql: "testdb.table1" `<br/>`- oracle: "test_schema.table1" `<br/>`- sqlserver: "testdb.test_schema.table1"` <br/>`- postgresql: "testdb.test_schema.table1"`  <br/>`- iris: "test_schema.table1"`                                                                                                                                                                                                                                                                                                                                                                                                  |
| table_list                                 | Array   | 否   | -       | 要读取的表列表，您可以使用此配置代替 `table_path`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| where_condition                            | String  | 否   | -       | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.size                                 | Int     | 否   | 8096    | 一个分割中有多少行，捕获的表在读取时被分成多个分割。**注意**：此参数仅在使用 `table_path` 参数时生效。使用 `query` 参数时不生效。                                                                                                                                                                                                                                                                                                                                                                                                         |
| common-options                             |         | 否   | -       | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。                                                                                                                                                                                                                                                                                                                                                                                                                                 |

### 表匹配

JDBC 源连接器支持两种方式指定表：

#### 注意事项

- 许多 JDBC 驱动会将 `DatabaseMetaData.getColumns(..., schemaPattern, tableNamePattern, ...)` 视为 SQL LIKE 的模式匹配。
  当 schema/table 名称中包含 `_` 或 `%` 时，列发现可能会返回其他表的列。SeaTunnel 会按精确的 schema/table 标识符对返回结果做二次过滤，
  以避免混入其他表的列。
- 对于大小写敏感的数据库，请确保配置的 schema/table 名称与数据库中实际标识符大小写一致。

1. **精确表路径**：使用 `table_path` 指定单个表及其完整路径。
   ```hocon
   table_path = "testdb.table1"
   ```

2. **正则表达式**：使用 `table_path` 与正则表达式模式匹配多个表。
   ```hocon
   table_path = "testdb.table\\d+"  # 匹配 table1, table2, table3 等
   use_regex = true
   ```

#### 表名的正则表达式支持

JDBC 连接器支持使用正则表达式匹配多个表。此功能允许您使用单个源配置处理多个表。

#### 配置

要对表路径使用正则表达式匹配：

1. 设置 `use_regex = true` 以启用正则表达式匹配
2. 如果未设置 `use_regex` 或设置为 `false`，连接器将把 table_path 视为精确路径（无正则表达式匹配）

#### 正则表达式语法注意事项

- **路径分隔符**：点 (`.`) 被视为数据库、模式和表名之间的分隔符。
- **转义点**：如果您需要在正则表达式中使用点 (`.`) 作为通配符来匹配任何字符，必须用反斜杠 (`\.`) 转义。
- **路径格式**：对于 `database.table` 或 `database.schema.table` 之类的路径，最后一个未转义的点将表模式与数据库/模式模式分开。
- **模式示例**：
  - `test.table\\d+` - 匹配 `test` 数据库中的 `table1`、`table2` 等表
  - `test.*` - 匹配 `test` 数据库中的所有表（用于整个数据库同步）
  - `postgres.public.test_db_\.*` - 匹配 `postgres` 数据库的 `public` 模式中以 `test_db_` 开头的所有表

#### 示例

```hocon
source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "password"

    table_list = [
      {
        # 正则表达式匹配 - 匹配 test 数据库中的任何表
        table_path = "test.*"
        use_regex = true
      },
      {
        # 正则表达式匹配 - 匹配名称为 "user" 后跟数字的表
        table_path = "test.user\\d+"
        use_regex = true
      },
      {
        # 精确匹配 - 简单表名
        table_path = "test.config"
        # use_regex 未指定，默认为 false
      },
    ]
  }
}
```

#### 多表同步

使用正则表达式时，连接器将从所有匹配的表中读取数据。每个表将被独立处理，数据将在输出中合并。

多表同步的示例配置：
```hocon
Jdbc {
    url = "jdbc:mysql://localhost/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"

    # 使用显式配置的正则表达式
    table_list = [
      {
        table_path = "testdb.table\\d+"
        use_regex = true
      }
    ]
}
```

### decimal_type_narrowing

十进制类型缩小，如果为 true，十进制类型将缩小为 int 或 long 类型（如果没有精度损失）。目前仅支持 Oracle。

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

### int_type_narrowing

Int 类型缩小，如果为 true，tinyint(1) 类型将缩小为布尔类型（如果没有精度损失）。目前支持 MySQL。

例如：

int_type_narrowing = true

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | Boolean   |

int_type_narrowing = false

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | TINYINT   |

### dialect [string]

指定的方言，如果不存在，仍然根据 url 获取，优先级高于 url。例如，使用 starrocks 时，需要将其设置为 `starrocks`。类似地，使用 mysql 时，需要将其值设置为 `mysql`。

如果 SeaTunnel 不支持某个方言，它将使用默认方言 `GenericDialect`。只需确保您提供的驱动程序支持您想要连接的数据库。

#### 方言列表

|           | 方言名称 |          |
|-----------|---------|----------|
| Greenplum | DB2     | Dameng   |
| Gbase8a   | HIVE    | KingBase |
| MySQL     | StarRocks | Oracle |
| Phoenix   | Postgres | Redshift |
| SapHana   | Snowflake | Sqlite |
| SqlServer | Tablestore | Teradata |
| Vertica   | OceanBase | XUGU |
| IRIS      | Inceptor | Highgo |

## 并行读取器

JDBC 源连接器支持从表中并行读取数据。SeaTunnel 将使用某些规则分割表中的数据，这些数据将交给读取器进行读取。读取器的数量由 `parallelism` 选项确定。

**分割键规则：**

1. 如果 `partition_column` 不为 null，它将用于计算分割。该列必须在**支持的分割数据类型**中。
2. 如果 `partition_column` 为 null，seatunnel 将从表中读取模式并获取主键和唯一索引。如果主键和唯一索引中有多个列，将使用**支持的分割数据类型**中的第一列来分割数据。例如，表有主键(nn guid, name varchar)，因为 `guid` 不在**支持的分割数据类型**中，所以列 `name` 将用于分割数据。

**支持的分割数据类型：**
* String
* Number(int, bigint, decimal, ...)
* Date

## 提示

> 如果表无法分割（例如，表没有主键或唯一索引，且未设置 `partition_column`），它将以单并发运行。
>
> 使用 `table_path` 替换 `query` 进行单表读取。如果需要读取多个表，请使用 `table_list`。
> 当基于 `query` 推断主键时，主键继承自结果集中第一列所在的底层表；如果 `query` 包含多表 JOIN 或同时从多张表读取，该主键对整个 JOIN 结果集的唯一性不作严格保证。

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

#### 情况 4 Oracle 源与 BLOB 作为字符串到 Doris Sink

此示例演示了在传输到 Doris 时如何将 Oracle 的 BLOB 数据作为字符串处理。这对于大型 BLOB 字段很有用。

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@oracle_host:1521/SERVICE_NAME"
    user = "username"
    password = "password"
    query = "SELECT ID, NAME, CONTENT_BLOB FROM MY_TABLE"
    handle_blob_as_string = true  # 为 Oracle 启用 BLOB 到字符串转换
  }
}
```

## 变更日志

<ChangeLog />


