import ChangeLog from '../changelog/connector-jdbc.md';

# DuckDB

> JDBC DuckDB 源连接器

## 描述

通过 JDBC 读取外部数据源数据。

## 支持 DuckDB 版本

- 0.8.x/0.9.x/0.10.x/1.x

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../concept/connector-v2-features.md)

> 支持 SQL 查询，并能实现列投影效果

## 支持的数据源信息

| 数据源    | 支持的版本              | 驱动器                     | 网址                               | Maven下载链接                                                       |
|--------|--------------------|-------------------------|----------------------------------|-----------------------------------------------------------------|
| DuckDB | 不同的依赖版本具有不同的驱动程序类。 | org.duckdb.DuckDBDriver | jdbc:duckdb:/path/to/database.db | [下载](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) |

## 数据类型映射

| DuckDB 数据类型                                              | SeaTunnel 数据类型 |
|----------------------------------------------------------|----------------|
| BOOLEAN                                                  | BOOLEAN        |
| TINYINT                                                  | TINYINT        |
| UTINYINT<br/>SMALLINT                                    | SMALLINT       |
| USMALLINT<br/>INTEGER                                    | INT            |
| UINTEGER<br/>BIGINT                                      | BIGINT         |
| UBIGINT                                                  | DECIMAL(20,0)  |
| HUGEINT                                                  | DECIMAL(38,0)  |
| FLOAT                                                    | FLOAT          |
| DOUBLE                                                   | DOUBLE         |
| DECIMAL(x,y)(获取指定列的指定列大小.<38)                            | DECIMAL(x,y)   |
| DECIMAL(x,y)(获取指定列的指定列大小.>38)                            | DECIMAL(38,18) |
| VARCHAR<br/>CHAR<br/>TEXT<br/>JSON<br/>UUID<br/>INTERVAL | STRING         |
| DATE                                                     | DATE           |
| TIME                                                     | TIME           |
| TIMESTAMP<br/>TIMESTAMP WITH TIME ZONE                   | TIMESTAMP      |
| BLOB<br/>ARRAY<br/>STRUCT<br/>MAP                        | BYTES          |

## 源选项

| 名称                           | 类型         | 是否必需 | 默认值             | 描述                                                                                                                                                   |
|------------------------------|------------|------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | 是    | -               | JDBC 连接的 URL。参考案例：jdbc:duckdb:/path/to/database.db                                                                                                   |
| driver                       | String     | 是    | -               | 用于连接到远程数据源的 jdbc 类名，<br/> 如果您使用 DuckDB，值为 `org.duckdb.DuckDBDriver`。                                                                                 |
| username                     | String     | 否    | -               | 连接实例用户名                                                                                                                                              |
| password                     | String     | 否    | -               | 连接实例密码                                                                                                                                               |
| query                        | String     | 是    | -               | 查询语句                                                                                                                                                 |
| connection_check_timeout_sec | Int        | 否    | 30              | 等待用于验证连接的数据库操作完成的时间（以秒为单位）                                                                                                                           |
| partition_column             | String     | 否    | -               | 并行度分区的列名，仅支持数字类型主键，并且只能配置一列。                                                                                                                         |
| partition_lower_bound        | BigDecimal | 否    | -               | 扫描的 partition_column 最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。                                                                                                |
| partition_upper_bound        | BigDecimal | 否    | -               | 扫描的 partition_column 最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。                                                                                                |
| partition_num                | Int        | 否    | job parallelism | 分区计数的数量，仅支持正整数。默认值为作业并行度                                                                                                                             |
| fetch_size                   | Int        | 否    | 0               | 对于返回大量对象的查询，您可以配置<br/> 查询中使用的行获取大小来通过<br/> 减少满足选择条件所需的数据库命中次数来提高性能。<br/> 零表示使用 jdbc 默认值。                                                             |
| properties                   | Map        | 否    | -               | 附加连接配置参数，当 properties 和 URL 具有相同参数时，优先级由 <br/>驱动程序的具体实现确定。例如，在 DuckDB 中，properties 优先于 URL。                                                          |
| table_path                   | String     | 否    | -               | 表的完整路径，您可以使用此配置代替 `query`。 <br/>示例： <br/>duckdb: "main.table1" <br/>                                                                                 |
| table_list                   | Array      | 否    | -               | 要读取的表列表，您可以使用此配置代替 `table_path` 示例：```[{ table_path = "main.table1"}, {table_path = "main.table2", query = "select * id, name from main.table2"}]``` |
| where_condition              | String     | 否    | -               | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`                                                                                                    |
| split.size                   | Int        | 否    | 8096            | 表的拆分大小（行数），读取表时捕获的表被拆分为多个拆分。                                                                                                                         |
| common-options               |            | 否    | -               | 源插件通用参数，详情请参考 [Source Common Options](../source-common-options.md)                                                                                   |

## 并行读取器

JDBC 源连接器支持从表中并行读取数据。SeaTunnel 将使用某些规则来拆分表中的数据，这些数据将交给读取器进行读取。读取器的数量由 `parallelism` 选项确定。

**拆分键规则：**

1. 如果 `partition_column` 不为空，它将用于计算拆分。该列必须在 **支持的拆分数据类型** 中。
2. 如果 `partition_column` 为空，seatunnel 将从表中读取模式并获取主键和唯一索引。如果主键和唯一索引中有多个列，将使用 **支持的拆分数据类型** 中的第一列来拆分数据。例如，表有主键(nn guid, name varchar)，因为 `guid` 不在 **支持的拆分数据类型** 中，所以列 `name` 将用于拆分数据。

**支持的拆分数据类型：**
* String
* Number(int, bigint, decimal, ...)
* Date

### 与拆分相关的选项

#### split.size

一个拆分中有多少行，读取表时捕获的表被拆分为多个拆分。

#### partition_column [string]

用于拆分数据的列名。

#### partition_upper_bound [BigDecimal]

扫描的 partition_column 最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。

#### partition_lower_bound [BigDecimal]

扫描的 partition_column 最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。

#### partition_num [int]

> 不建议使用，正确的方法是通过 `split.size` 控制拆分数量

我们需要拆分成多少个拆分，仅支持正整数。默认值为作业并行度。

## 提示

> 如果表无法拆分（例如，表没有主键或唯一索引，并且未设置 `partition_column`），它将以单一并发运行。
>
> 使用 `table_path` 替换 `query` 进行单表读取。如果您需要读取多个表，请使用 `table_list`。

## 任务示例

### 简单

> 此示例在单个并行中查询测试数据库中的 'user_events' 表并查询其所有字段。您还可以指定要查询的字段以最终输出到控制台。

```
# 定义运行时环境
env {
  parallelism = 4
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = "duckdb"
        password = ""
        query = "select * from user_events limit 16"
    }
}

transform {
    # 如果您想了解更多关于如何配置 seatunnel 和查看转换插件的完整列表，
    # 请访问 https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### 通过 partition_column 并行

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = "duckdb"
        password = ""
        query = "select * from user_events"
        partition_column = "id"
        split.size = 10000
        # 读取开始边界
        #partition_lower_bound = ...
        # 读取结束边界
        #partition_upper_bound = ...
    }
}

sink {
  Console {}
}
```

### 通过主键或唯一索引并行

> 配置 `table_path` 将开启自动拆分，您可以配置 `split.*` 来调整拆分策略

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = "duckdb"
        password = ""
        table_path = "main.user_events"
        query = "select * from main.user_events"
        split.size = 10000
    }
}

sink {
  Console {}
}
```

### 并行边界

> 指定查询的上下边界内的数据更高效，根据您配置的上下边界读取数据源更高效

```
source {
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = "duckdb"
        password = ""
        # 根据需要定义查询逻辑
        query = "select * from user_events"
        partition_column = "id"
        # 读取开始边界
        partition_lower_bound = 1
        # 读取结束边界
        partition_upper_bound = 500
        partition_num = 10
        properties {
         threads=4
         memory_limit="4GB"
        }
    }
}
```

### 多表读取

***配置 `table_list` 将开启自动拆分，您可以配置 `split.*` 来调整拆分策略***

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}
source {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    connection_check_timeout_sec = 100
    username = "duckdb"
    password = ""

    table_list = [
      {
        table_path = "main.table1"
      },
      {
        table_path = "main.table2"
        # 使用查询过滤行和列
        query = "select id, name from main.table2 where id > 100"
      }
    ]
    #where_condition= "where id > 100"
    #split.size = 8096
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />