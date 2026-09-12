import ChangeLog from '../changelog/connector-jdbc.md';

# MySQL

> JDBC MySQL 源连接器

## 描述

通过 JDBC 读取 MySQL 数据。本连接器继承了 [Jdbc 源连接器](./Jdbc.md) 的全部选项，使用官方
MySQL 驱动（`com.mysql.cj.jdbc.Driver`）。

支持批处理模式（基于拆分键的并行读取）。如需增量快照或变更数据捕获语义，请使用
[MySQL-CDC 源连接器](./MySQL-CDC.md)。

## 支持的 MySQL 版本

- 5.5 / 5.6 / 5.7 / 8.0 / 8.1 / 8.2 / 8.3 / 8.4

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

> 支持 SQL 查询，并能实现列投影效果。如需变更数据捕获，请使用 [MySQL-CDC 连接器](./MySQL-CDC.md)。

## 支持的数据源信息

| 数据源 | 支持的版本                                       | 驱动类                  | URL                                | Maven 下载                                                                       |
|--------|--------------------------------------------------|-------------------------|------------------------------------|----------------------------------------------------------------------------------|
| MySQL  | 不同的依赖版本对应不同的驱动类。                 | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test   | [下载](https://mvnrepository.com/artifact/mysql/mysql-connector-java)              |

## 数据类型映射

| MySQL 数据类型                                                                                | SeaTunnel 数据类型                                                                                |
|-----------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| BIT(1)<br/>TINYINT(1)                                                                         | BOOLEAN                                                                                            |
| TINYINT                                                                                       | BYTE                                                                                               |
| TINYINT UNSIGNED<br/>SMALLINT                                                                 | SMALLINT                                                                                           |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR           | INT                                                                                                |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                  | BIGINT                                                                                             |
| BIGINT UNSIGNED                                                                               | DECIMAL(20,0)                                                                                      |
| DECIMAL(x,y)（列大小 < 38）                                                                   | DECIMAL(x,y)                                                                                       |
| DECIMAL(x,y)（列大小 > 38）                                                                   | DECIMAL(38,18)                                                                                     |
| DECIMAL UNSIGNED                                                                              | DECIMAL((获取指定列的列大小)+1,<br/>(获取指定列的小数点右侧的位数))                                    |
| FLOAT<br/>FLOAT UNSIGNED                                                                      | FLOAT                                                                                              |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                    | DOUBLE                                                                                             |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM          | STRING                                                                                             |
| DATE                                                                                          | DATE                                                                                               |
| TIME(s)                                                                                       | TIME(s)                                                                                            |
| DATETIME<br/>TIMESTAMP(s)                                                                     | TIMESTAMP(s)                                                                                       |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)<br/>GEOMETRY | BYTES                                                                                              |

## 源选项

本连接器使用的选项与 [Jdbc 源连接器](./Jdbc.md#source-options) 相同。下表给出了本连接器
暴露的全部选项；未列出项请参考通用 JDBC 源的说明。

| 名称                                          | 类型       | 是否必填 | 默认值         | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|-----------------------------------------------|------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                           | String     | 是       | -              | JDBC 连接 URL。示例：`jdbc:mysql://localhost:3306/test`。较大数据量读取时建议附带 `serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true`。                                                                                                                                                                                                                                                                                                                                  |
| driver                                        | String     | 是       | -              | 用于连接远程数据源的 JDBC 类名。MySQL 使用 `com.mysql.cj.jdbc.Driver`。                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| username                                      | String     | 是       | -              | 连接实例的用户名。同时接受 `user` 作为 `username` 的别名。                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| password                                      | String     | 是       | -              | 连接实例的密码。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| query                                         | String     | 否       | -              | 查询语句。当未配置 `table_path` 和 `table_list` 时必填。                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connection_check_timeout_sec                  | Int        | 否       | 30             | 验证数据库连接完成所需等待的秒数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| partition_column                              | String     | 否       | -              | 用于并行度分区的列名，仅支持数字类型的主键，且只能配置一列。                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| partition_lower_bound                         | BigDecimal | 否       | -              | 扫描时 `partition_column` 的最小值；未设置时 SeaTunnel 会查询数据库获取最小值。                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| partition_upper_bound                         | BigDecimal | 否       | -              | 扫描时 `partition_column` 的最大值；未设置时 SeaTunnel 会查询数据库获取最大值。                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| partition_num                                 | Int        | 否       | 作业并行度      | 分区数量，仅支持正整数；默认与作业并行度一致。                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| fetch_size                                    | Int        | 否       | 0              | 对返回大量对象的查询，配置行获取大小以减少满足选择条件所需的数据库访问次数。设置为 0 表示使用 JDBC 驱动默认值。                                                                                                                                                                                                                                                                                                                                                                                                     |
| properties                                    | Map        | 否       | -              | 额外的连接配置参数。当 `properties` 和 URL 存在相同参数时，优先级由驱动决定；在 MySQL 中，`properties` 优先于 URL。                                                                                                                                                                                                                                                                                                                                                                                                |
| use_regex                                     | Boolean    | 否       | false          | 控制 `table_path` 是否按正则匹配。`true` 时按正则模式匹配；`false` 或未设置时按精确路径匹配（不进行正则匹配）。                                                                                                                                                                                                                                                                                                                                                                                                    |
| table_path                                    | String     | 否       | -              | 表的完整路径，可代替 `query`。示例：`"testdb.table1"`。                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| table_list                                    | Array      | 否       | -              | 需要读取的表列表，可代替 `table_path`。示例：`[{table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select id, name from testdb.table2"}]`。                                                                                                                                                                                                                                                                                                                                                    |
| where_condition                               | String     | 否       | -              | 对所有表/查询生效的通用行过滤条件，必须以 `where` 开头，例如 `where id > 100`。                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| split.size                                    | Int        | 否       | 8096           | 单个分片的行数；捕获到的表在读取时会被拆分为多个分片。                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| split.even-distribution.factor.lower-bound    | Double     | 否       | 0.05           | 分片键分布因子的下限。用于判断表数据是否均匀分布。当分布因子 `(MAX(id) - MIN(id) + 1) / row count` 大于等于该下限时，表分片会被优化为均匀分布；否则视为不均匀分布，当估算分片数超过 `sample-sharding.threshold` 时启用采样分片策略。                                                                                                                                                                                                                                                                            |
| split.even-distribution.factor.upper-bound    | Double     | 否       | 100.0          | 分片键分布因子的上限。当分布因子小于等于该上限时，表分片会被优化为均匀分布；否则视为不均匀分布，当估算分片数超过 `sample-sharding.threshold` 时启用采样分片策略。                                                                                                                                                                                                                                                                                                                                                    |
| split.sample-sharding.threshold               | Int        | 否       | 1000           | 触发采样分片策略的估算分片数阈值。当分布因子落在 `split.even-distribution.factor.upper-bound` 与 `split.even-distribution.factor.lower-bound` 之外，且估算分片数（大致行数 / split.size）超过该阈值时启用采样分片策略。                                                                                                                                                                                                                                                                                       |
| split.allow-sampling                          | Boolean    | 否       | true           | 是否允许对分布不均匀的分片键使用采样分片策略。`false` 时 SeaTunnel 回退到迭代式不均匀分片。                                                                                                                                                                                                                                                                                                                                                                                                                       |
| use_select_count                              | Boolean    | 否       | false          | 是否在分片前使用 `select count(*)` 估算表行数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| skip_analyze                                  | Boolean    | 否       | false          | 是否跳过分片前的表行数分析。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| split.inverse-sampling.rate                   | Int        | 否       | 1000           | 采样分片策略中采样率的倒数。例如设置为 1000 表示采样率为 1/1000。处理超大数据集时调高此值可降低采样率。                                                                                                                                                                                                                                                                                                                                                                                                            |
| int_type_narrowing                            | Boolean    | 否       | true           | 整型收窄。当为 `true` 且无精度损失时，`tinyint(1)` 会被收窄为 `boolean`。目前仅 MySQL 支持。                                                                                                                                                                                                                                                                                                                                                                                                                       |
| common-options                                |            | 否       | -              | 源插件通用参数，详情请参考 [Source Common Options](../common-options/source-common-options.md)。                                                                                                                                                                                                                                                                                                                                                                                                                  |

### int_type_narrowing

`int_type_narrowing` 控制 MySQL 的 `tinyint(1)` 映射到 SeaTunnel 类型的方式：

- `int_type_narrowing = true`（默认）：`tinyint(1)` → `boolean`。
- `int_type_narrowing = false`：`tinyint(1)` → `tinyint`。

## 并行读取

JDBC 源连接器支持并行读取表数据。SeaTunnel 按照一定规则拆分数据并交给读取器处理，读取器
数量由 `parallelism` 决定。

**拆分键规则：**

1. 如果 `partition_column` 不为空，则使用该列进行拆分；该列必须属于下面的**支持拆分数据类型**。
2. 如果 `partition_column` 为空，SeaTunnel 会读取表结构并选取主键或唯一索引。当主键或唯一索引
   包含多列时，按顺序选取第一列属于**支持拆分数据类型**的列进行拆分。例如表的主键为
   `(guid, name varchar)`，其中 `guid` 不属于支持拆分数据类型，则使用 `name` 列进行拆分。

**支持拆分数据类型：**

- String
- Number（`int`、`bigint`、`decimal` 等）
- Date

## 提示

> 如果表无法拆分（例如表没有主键或唯一索引，且 `partition_column` 未设置），将以单并发方式运行。
>
> 单表读取时可使用 `table_path` 代替 `query`；读取多张表时使用 `table_list`。
>
> 当基于 `query` 推断主键时，主键继承自结果集中第一列所在的底层表；如果 `query` 包含 JOIN
> 或同时从多张表读取，该主键对整个 JOIN 结果集的唯一性不作严格保证。
>
> 如需增量快照或变更数据捕获语义，请使用 [MySQL-CDC 源连接器](./MySQL-CDC.md)。

## 任务示例

### 简单示例

> 以单线程并行方式查询测试库中 `type_bin` 表的 16 条数据，并选择全部字段；也可以指定输出到
> 控制台的列。

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    query = "select * from type_bin limit 16"
  }
}

transform {
}

sink {
  Console {}
}
```

### 按 `partition_column` 并行

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    query = "select * from type_bin"
    partition_column = "id"
    split.size = 10000
    # 起始边界；不填则由 SeaTunnel 查询数据库获取最小值
    # partition_lower_bound = ...
    # 结束边界；不填则由 SeaTunnel 查询数据库获取最大值
    # partition_upper_bound = ...
  }
}

sink {
  Console {}
}
```

### 按主键或唯一索引并行

> 配置 `table_path` 会自动开启拆分。可通过 `split.*` 选项调整拆分策略。

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    table_path = "testdb.table1"
    query = "select * from testdb.table1"
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### 指定上下边界的并行读取

> 同时指定查询范围的上下边界是驱动并行读取最高效的方式。

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    query = "select * from type_bin"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 500
    partition_num = 10
    properties {
      useSSL = "false"
    }
  }
}

sink {
  Console {}
}
```

### 多表读取

> 配置 `table_list` 会自动开启拆分。可通过 `split.*` 选项调整拆分策略。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"

    table_list = [
      {
        table_path = "testdb.table1"
      },
      {
        table_path = "testdb.table2"
        # 使用 query 过滤行和列
        query = "select id, name from testdb.table2 where id > 100"
      }
    ]
    # where_condition = "where id > 100"
    # split.size = 8096
    # split.even-distribution.factor.upper-bound = 100
    # split.even-distribution.factor.lower-bound = 0.05
    # split.sample-sharding.threshold = 1000
    # split.inverse-sampling.rate = 1000
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />