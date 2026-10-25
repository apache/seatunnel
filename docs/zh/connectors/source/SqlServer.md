import ChangeLog from '../changelog/connector-jdbc.md';

# SQL Server

> JDBC SQL Server 源连接器

## 支持 SQL Server 版本

- server:2008（或更高版本，仅供参考）

## 支持的引擎

> Spark <br/>
> Flink <br/>
> Seatunnel Zeta <br/>

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已经放置在 `${SEATUNNEL_HOME}/plugins/` 目录中。

### 对于 SeaTunnel Zeta 引擎

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已经放置在 `${SEATUNNEL_HOME}/lib/` 目录中。

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户定义分割](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL 并可以实现投影效果。

## 描述

通过 JDBC 读取外部数据源数据。SQL Server 的更多通用选项请参考 [Jdbc 连接器](Jdbc.md)。

## 支持的数据源信息

| 数据源 | 支持版本 | 驱动 | url | maven |
|--------|----------|------|-----|-------|
| SQL Server | 支持版本 >= 2008 | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `jdbc:sqlserver://localhost:1433` | [下载](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 工作目录。<br/>
> 例如 SQL Server 数据源：`cp mssql-jdbc-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`

## 数据类型映射

| SQL Server 数据类型 | SeaTunnel 数据类型 |
|---------------------|---------------------|
| BIT | BOOLEAN |
| TINYINT / SMALLINT | SMALLINT |
| INTEGER / INT | INT |
| BIGINT | BIGINT |
| NUMERIC(p,s) / DECIMAL(p,s) / MONEY / SMALLMONEY | DECIMAL(p,s) |
| FLOAT(1~24) / REAL | FLOAT |
| DOUBLE / FLOAT(>24) | DOUBLE |
| CHAR / NCHAR / VARCHAR / NTEXT / NVARCHAR / TEXT / XML | STRING |
| DATE | DATE |
| TIME(s) | TIME(s) |
| DATETIME / DATETIME2 / DATETIMEOFFSET / SMALLDATETIME | TIMESTAMP(s) |
| BINARY / VARBINARY / IMAGE | BYTES |

## 数据源参数

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| url | String | 是 | - | JDBC 连接的 URL，例如 `jdbc:sqlserver://127.0.0.1:1434;database=TestDB`。 |
| driver | String | 是 | - | 用于连接远程数据源的 JDBC 类名，SQL Server 使用 `com.microsoft.sqlserver.jdbc.SQLServerDriver`。 |
| username | String | 否 | - | 连接实例的用户名。 |
| password | String | 否 | - | 连接实例的密码。 |
| query | String | 否 | - | 查询语句。当未配置 `table_path` 和 `table_list` 时必填。 |
| connection_check_timeout_sec | Int | 否 | 30 | 等待用于验证连接的数据库操作完成的时间（秒）。 |
| partition_column | String | 否 | - | 用于并行度分区的列名，仅支持数值类型。 |
| partition_lower_bound | Long | 否 | - | `partition_column` 扫描的最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。 |
| partition_upper_bound | Long | 否 | - | `partition_column` 扫描的最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。 |
| partition_num | Int | 否 | job parallelism | 分区数量，仅支持正整数。默认值为作业并行度。 |
| fetch_size | Int | 否 | 0 | 查询使用的行获取大小。`0` 表示使用 JDBC 默认值。增大可减少对数据库的命中次数。 |
| properties | Map | 否 | - | 额外的连接配置参数。当 properties 与 URL 含相同参数时，由驱动的具体实现决定优先级。 |
| use_regex | Boolean | 否 | false | 控制 `table_path` 的正则匹配。`true` 时按正则匹配，`false`（默认）时按精确路径匹配。 |
| table_path | String | 否 | - | 表的完整路径，可用于替代 `query`。示例：`testdb.test_schema.table1`。 |
| table_list | Array | 否 | - | 要读取的表列表，可替代 `table_path`。示例：`[{ table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select id, name from testdb.table2"}]`。 |
| where_condition | String | 否 | - | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`。 |
| split.size | Int | 否 | 8096 | 表的分割大小（行数），读取表时会被分割为多个 split。 |
| split.even-distribution.factor.lower-bound | Double | 否 | 0.05 | 分块键分布因子的下界。`(MAX(id) - MIN(id) + 1) / 行数` 大于等于该下界时认为表分布均匀；否则进入采样分片路径。 |
| split.even-distribution.factor.upper-bound | Double | 否 | 100 | 分块键分布因子的上界。上界取值的逻辑与下界对称。 |
| split.sample-sharding.threshold | Int | 否 | 1000 | 触发采样分片策略的估算分片数阈值。 |
| split.allow-sampling | Boolean | 否 | true | 是否允许对分布不均匀的分片键使用采样分片策略。`false` 时退回到迭代式不均匀分片。 |
| use_select_count | Boolean | 否 | false | 是否在分片前用 `select count(*)` 估算表行数。 |
| skip_analyze | Boolean | 否 | false | 是否跳过分片前的表行数分析。 |
| split.inverse-sampling.rate | Int | 否 | 1000 | 采样分片策略中采样率的倒数。`1000` 表示 1/1000 的采样率。 |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md)。 |

## 并行读取器

JDBC 源连接器支持从表中并行读取数据。SeaTunnel 将使用某些规则来分割表中的数据，然后交给读取器进行读取。读取器的数量由 `parallelism` 选项决定。

**分割键规则：**

1. 如果 `partition_column` 不为空，将使用它来计算分割。该列必须在 **支持的分割数据类型** 中。
2. 如果 `partition_column` 为空，SeaTunnel 将从表中读取模式并获取主键和唯一索引。如果主键和唯一索引中有多个列，则将使用 **支持的分割数据类型** 中的第一列来分割数据。

**支持的分割数据类型：**

* String
* Number（int、bigint、decimal 等）
* Date

### 分割相关选项

#### split.size

一个 split 中包含多少行。表被读入时会被分割为多个 split。

#### split.even-distribution.factor.lower-bound

> 不推荐使用

分块键分布因子的下界。`(MAX(id) - MIN(id) + 1) / 行数` 大于等于该下界时认为表分布均匀；否则进入采样分片路径。默认值为 0.05。

#### split.even-distribution.factor.upper-bound

> 不推荐使用

分块键分布因子的上界。逻辑与下界对称。默认值为 100.0。

#### split.sample-sharding.threshold

触发采样分片策略的估算分片数阈值。默认值为 1000。

#### split.inverse-sampling.rate

采样分片策略中采样率的倒数。默认值为 1000（1/1000 的采样率）。

#### partition_column [string]

用于分割数据的列名。

#### partition_upper_bound [BigDecimal]

`partition_column` 扫描的最大值。如果未设置，SeaTunnel 将查询数据库获取最大值。

#### partition_lower_bound [BigDecimal]

`partition_column` 扫描的最小值。如果未设置，SeaTunnel 将查询数据库获取最小值。

#### partition_num [int]

> 不推荐使用，正确的方法是通过 `split.size` 控制分割数量

需要分割为多少个 split，仅支持正整数。默认值为作业并行度。

## 提示

> 如果表无法分割（例如，表没有主键或唯一索引，且未设置 `partition_column`），将以单并发运行。
>
> 单表读取可使用 `table_path` 替代 `query`，多表读取请使用 `table_list`。

## 任务示例

### 简单的例子

> 读取数据表的简单单个任务

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    query = "select * from full_types_jdbc"
  }
}

transform {
  # 如果你想了解更多关于如何配置 seatunnel 的信息，并查看转换插件的完整列表，
  # 请前往 https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

### 并行示例

> 使用您配置的分区字段并行读取数据。如果需要读取整张表，可以结合 `query` 或 `table_path` 使用。

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    query = "select * from full_types_jdbc"
    partition_column = "id"
    partition_num = 10
  }
}

transform {
  # 如果你想了解更多关于如何配置 seatunnel 的信息，并查看转换插件的完整列表，
  # 请前往 https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

### 整库多表读取

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    table_list = [
      { table_path = "column_type_test.dbo.full_types_jdbc" }
      { table_path = "column_type_test.dbo.orders", query = "select id, name, status from column_type_test.dbo.orders" }
    ]
    where_condition = "where id > 0"
    split.size = 10000
  }
}

transform {
  Sql {
    plugin_input = "Jdbc"
    plugin_output = "tmp_id_name"
    query = "select id, name from full_types_jdbc"
  }
}

sink {
  Console {}
}
```

### 自定义列裁剪示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    table_path = "column_type_test.dbo.full_types_jdbc"
    query = "select id, name from column_type_test.dbo.full_types_jdbc"
  }
}

transform {
}

sink {
  Jdbc {
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    generate_sink_sql = true
    database = "column_type_test"
    table = "dbo.full_types_jdbc"
  }
}
```

## 变更日志

<ChangeLog />
