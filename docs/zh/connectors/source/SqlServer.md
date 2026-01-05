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

> 支持查询 SQL 并可以实现投影效果。

## 描述

通过 JDBC 读取外部数据源数据。

## 支持的数据源信息

| 数据源     |   支持版本              |                    驱动                      |               url               |                                       maven                                       |
|------------|-------------------------|----------------------------------------------|---------------------------------|-----------------------------------------------------------------------------------|
| SQL Server | 支持版本 >= 2008        | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433 | [下载](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录<br/>
> 例如 SQL Server 数据源：cp mssql-jdbc-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|                         SQLserver 数据类型                           | Seatunnel 数据类型   |
|----------------------------------------------------------------------|---------------------|
| BIT                                                                  | BOOLEAN             |
| TINYINT<br/>SMALLINT                                                 | SMALLINT            |
| INTEGER<br/>INT                                                      | INT                 |
| BIGINT                                                               | BIGINT              |
| NUMERIC(p,s)<br/>DECIMAL(p,s)<br/>MONEY<br/>SMALLMONEY               | DECIMAL(p,s)        |
| FLOAT(1~24)<br/>REAL                                                 | FLOAT               |
| DOUBLE<br/>FLOAT(>24)                                                | DOUBLE              |
| CHAR<br/>NCHAR<br/>VARCHAR<br/>NTEXT<br/>NVARCHAR<br/>TEXT<br/>XML   | STRING              |
| DATE                                                                 | DATE                |
| TIME(s)                                                              | TIME(s)             |
| DATETIME(s)<br/>DATETIME2(s)<br/>DATETIMEOFFSET(s)<br/>SMALLDATETIME | TIMESTAMP(s)        |
| BINARY<br/>VARBINARY<br/>IMAGE                                       | BYTES               |

## 数据源参数

| 名称                                       | 类型    | 是否必填 | 默认值          | 描述                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------ | ------- | -------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| url                                        | String  | 是       | -               | JDBC 连接的 URL。参见示例：jdbc:sqlserver://127.0.0.1:1434;database=TestDB                                                                                                                                                                                                                                          |
| driver                                     | String  | 是       | -               | 用于连接远程数据源的 jdbc 类名，<br/>如果使用 SQLserver，值为 `com.microsoft.sqlserver.jdbc.SQLServerDriver`。                                                                                                                                                                                                      |
| username                                   | String  | 否       | -               | 连接实例的用户名                                                                                                                                                                                                                                                                                                    |
| password                                   | String  | 否       | -               | 连接实例的密码                                                                                                                                                                                                                                                                                                      |
| query                                      | String  | 是       | -               | 查询语句                                                                                                                                                                                                                                                                                                            |
| connection_check_timeout_sec               | Int     | 否       | 30              | 等待用于验证连接的数据库操作完成的时间（秒）                                                                                                                                                                                                                                                                        |
| partition_column                           | String  | 否       | -               | 用于并行度分区的列名，仅支持数值类型。                                                                                                                                                                                                                                                                              |
| partition_lower_bound                      | Long    | 否       | -               | partition_column 扫描的最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。                                                                                                                                                                                                                                       |
| partition_upper_bound                      | Long    | 否       | -               | partition_column 扫描的最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。                                                                                                                                                                                                                                       |
| partition_num                              | Int     | 否       | job parallelism | 分区数量，仅支持正整数。默认值为作业并行度                                                                                                                                                                                                                                                                          |
| fetch_size                                 | Int     | 否       | 0               | 对于返回大量对象的查询，你可以配置<br/>查询中使用的行获取大小来提高性能，<br/>通过减少满足选择条件所需的数据库命中次数。<br/>零表示使用 jdbc 默认值。                                                                                                                                                               |
| properties                                 | Map     | 否       | -               | 额外的连接配置参数，当 properties 和 URL 具有相同参数时，优先级由<br/>驱动的具体实现决定。例如，在 MySQL 中，properties 优先于 URL。                                                                                                                                                                                |
| use_regex                                  | Boolean | 否       | false           | 控制 table_path 的正则表达式匹配。当设置为 `true` 时，table_path 将被视为正则表达式模式。当设置为 `false` 或未指定时，table_path 将被视为精确路径（不进行正则匹配）。                                                                                                                                               |
| table_path                                 | String  | 否       | -               | 表的完整路径，您可以使用此配置代替 `query`。<br/>示例：<br/>"testdb.test_schema.table1"                                                                                                          |
| table_list                                 | Array   | 否       | -               | 要读取的表列表，您可以使用此配置代替 `table_path`。示例：```[{ table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select * id, name from testdb.table2"}]```                                                                                                                                    |
| where_condition                            | String  | 否       | -               | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`                                                                                                                                                                                                                                             |
| split.size                                 | Int     | 否       | 8096            | 表的分割大小（行数），读取表时，捕获的表会被分割为多个分割。                                                                                                                                                                                                                                                        |
| split.even-distribution.factor.lower-bound | Double  | 否       | 0.05            | 分块键分布因子的下界。此因子用于确定表数据是否均匀分布。如果计算的分布因子大于或等于此下界（即，(MAX(id) - MIN(id) + 1) / 行数），表分块将被优化以实现均匀分布。否则，如果分布因子较小，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，表将被视为不均匀分布并使用基于采样的分片策略。默认值为 0.05。    |
| split.even-distribution.factor.upper-bound | Double  | 否       | 100             | 分块键分布因子的上界。此因子用于确定表数据是否均匀分布。如果计算的分布因子小于或等于此上界（即，(MAX(id) - MIN(id) + 1) / 行数），表分块将被优化以实现均匀分布。否则，如果分布因子较大，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，表将被视为不均匀分布并使用基于采样的分片策略。默认值为 100.0。   |
| split.sample-sharding.threshold            | Int     | 否       | 10000           | 此配置指定了触发采样分片策略的估计分片数阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且估计的分片数（计算为近似行数 / 分块大小）超过此阈值时，将使用采样分片策略。这可以帮助更有效地处理大型数据集。默认值为 1000 分片。 |
| split.inverse-sampling.rate                | Int     | 否       | 1000            | 采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。对于非常大的数据集，首选较低的采样率时，此选项特别有用。默认值为 1000。                                                                            |
| common-options                             |         | 否       | -               | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 获取详细信息                                                                                                                                                                                                                                       |

## 并行读取器

JDBC 源连接器支持从表中并行读取数据。SeaTunnel 将使用某些规则来分割表中的数据，然后将其交给读取器进行读取。读取器的数量由 `parallelism` 选项决定。

**分割键规则：**

1. 如果 `partition_column` 不为空，将使用它来计算分割。该列必须在 **支持的分割数据类型** 中。
2. 如果 `partition_column` 为空，seatunnel 将从表中读取模式并获取主键和唯一索引。如果主键和唯一索引中有多个列，则将使用 **支持的分割数据类型** 中的第一列来分割数据。例如，表具有主键(nn guid, name varchar)，因为 `guid` 不在 **支持的分割数据类型** 中，所以将使用 `name` 列来分割数据。

**支持的分割数据类型：**
* String
* Number(int, bigint, decimal, ...)
* Date

### 与分割相关的选项

#### split.size

一个分割中有多少行，读取表时，捕获的表会被分割为多个分割。

#### split.even-distribution.factor.lower-bound

> 不推荐使用

分块键分布因子的下界。此因子用于确定表数据是否均匀分布。如果计算的分布因子大于或等于此下界（即，(MAX(id) - MIN(id) + 1) / 行数），表分块将被优化以实现均匀分布。否则，如果分布因子较小，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，表将被视为不均匀分布并使用基于采样的分片策略。默认值为 0.05。

#### split.even-distribution.factor.upper-bound

> 不推荐使用

分块键分布因子的上界。此因子用于确定表数据是否均匀分布。如果计算的分布因子小于或等于此上界（即，(MAX(id) - MIN(id) + 1) / 行数），表分块将被优化以实现均匀分布。否则，如果分布因子较大，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，表将被视为不均匀分布并使用基于采样的分片策略。默认值为 100.0。

#### split.sample-sharding.threshold

此配置指定了触发采样分片策略的估计分片数阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且估计的分片数（计算为近似行数 / 分块大小）超过此阈值时，将使用采样分片策略。这可以帮助更有效地处理大型数据集。默认值为 1000 分片。

#### split.inverse-sampling.rate

采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。对于非常大的数据集，首选较低的采样率时，此选项特别有用。默认值为 1000。

#### partition_column [string]

用于分割数据的列名。

#### partition_upper_bound [BigDecimal]

partition_column 扫描的最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。

#### partition_lower_bound [BigDecimal]

partition_column 扫描的最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。

#### partition_num [int]

> 不推荐使用，正确的方法是通过 `split.size` 控制分割数量

我们需要分割为多少个分割，仅支持正整数。默认值为作业并行度。

## 提示

> 如果表无法分割（例如，表没有主键或唯一索引，且未设置 `partition_column`），将以单个并发运行。
>
> 使用 `table_path` 替代 `query` 进行单表读取。如果需要读取多个表，请使用 `table_list`。

## 任务示例

### 简单的例子

> 读取数据表的简单单个任务

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}
source{
    Jdbc {
        driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
        url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
        username = SA
        password = "Y.sa123456"
        query = "select * from full_types_jdbc"
    }
}

transform {
    # 如果你想了解更多关于如何配置 seatunnel 的信息，并查看转换插件的完整列表，
    # 请前往 https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### 并行示例

> 使用您配置的分片字段并行读取查询表和分片数据。如果您想读取整个表，可以这样做

```
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
    Jdbc {
        driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
        url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
        username = SA
        password = "Y.sa123456"
        # 根据需要定义查询逻辑
        query = "select * from full_types_jdbc"
        # 并行分片读取字段
        partition_column = "id"
        # 分片数量
        partition_num = 10
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}

```

### 分片并行读取简单示例

> 这是一个快速并行读取数据的分片

```
env {
  # 您可以在这里设置引擎配置
  parallelism = 10
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = SA
    password = "Y.sa123456"
    query = "select * from column_type_test.dbo.full_types_jdbc"
    # 并行分片读取字段
    partition_column = "id"
    # 分片数量
    partition_num = 10

  }
  # 如果你想了解更多关于如何配置 seatunnel 的信息，并查看源插件的完整列表，
  # 请前往 https://seatunnel.apache.org/docs/connector-v2/source/Jdbc
}


transform {
  # 如果你想了解更多关于如何配置 seatunnel 的信息，并查看转换插件的完整列表，
  # 请前往 https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
  Console {}
  # 如果你想了解更多关于如何配置 seatunnel 的信息，并查看汇插件的完整列表，
  # 请前往 https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc
}
```

## 变更日志

<ChangeLog />