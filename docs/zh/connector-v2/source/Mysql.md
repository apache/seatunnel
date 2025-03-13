# MySQL

> JDBC Mysql 源连接器

## 描述

通过 JDBC 读取外部数据源数据。

## 支持 Mysql 版本

- 5.5/5.6/5.7/8.0/8.1/8.2/8.3/8.4

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../concept/connector-v2-features.md)
- [x] [支持多表读取](../../concept/connector-v2-features.md)

> 支持 SQL 查询，并能实现列投影效果

## 支持的数据源信息

| 数据源 |                    支持的版本                   |          驱动器          |                  网址                  | Maven下载链接                                                           |
|-----|---------------------------------------------------------|--------------------------|---------------------------------------|---------------------------------------------------------------------|
| Mysql | 不同的依赖版本具有不同的驱动程序类。 | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306:3306/test | [下载](https://mvnrepository.com/artifact/mysql/mysql-connector-java) |

## 数据类型映射

| Mysql 数据类型                                                                                  |                                                                 SeaTunnel 数据类型                                                             |
|---------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>TINYINT(1)                                                                       | BOOLEAN                                                                                                                                         |
| TINYINT                                                                                     | BYTE                                                                                                                                            |
| TINYINT UNSIGNED<br/>SMALLINT                                                               | SMALLINT                                                                                                                                        |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR         | INT                                                                                                                                             |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                | BIGINT                                                                                                                                          |
| BIGINT UNSIGNED                                                                             | DECIMAL(20,0)                                                                                                                                   |
| DECIMAL(x,y)(获取指定列的列大小<38)                                                                  | DECIMAL(x,y)                                                                                                                                    |
| DECIMAL(x,y)(获取指定列的列大小>38)                                                                  | DECIMAL(38,18)                                                                                                                                  |
| DECIMAL UNSIGNED                                                                            | DECIMAL((获取指定列的列大小)+1,<br/>(获取指定列的小数点右侧的位数)) |
| FLOAT<br/>FLOAT UNSIGNED                                                                    | FLOAT                                                                                                                                           |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                  | DOUBLE                                                                                                                                          |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM        | STRING                                                                                                                                          |
| DATE                                                                                        | DATE                                                                                                                                            |
| TIME(s)                                                                                     | TIME(s)                                                                                                                                         |
| DATETIME<br/>TIMESTAMP(s)                                                                   | TIMESTAMP(s)                                                                                                                                    |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)<br/>GEOMETRY | BYTES                                                                                                                                           |

## 数据源参数

|                    名称                    | 类型       | 是否必填 | 默认值         | 描述                                                                                                                                                                                                                     |
|--------------------------------------------|------------|------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String     | 是    | -               | JDBC 连接的 URL。参见示例: <br/>`jdbc:mysql://localhost:3306:3306/test`。                                                                                                                                                       |
| driver                                     | String     | 是    | -               | 用于连接远程数据源的 JDBC 类名，<br/>如果使用 MySQL，值为 `com.mysql.cj.jdbc.Driver`。                                                                                                                                                      |
| user                                       | String     | 否    | -               | 连接实例用户名。                                                                                                                                                                                                               |
| password                                   | String     | 否    | -               | 连接实例密码。                                                                                                                                                                                                                |
| query                                      | String     | 是    | -               | 查询语句。                                                                                                                                                                                                                  |
| connection_check_timeout_sec               | Int        | 否    | 30              | 验证数据库连接所使用的操作完成的等待时间（秒）。                                                                                                                                                                                               |
| partition_column                           | String     | 否    | -               | 用于并行度分区的列名，仅支持数字类型，仅支持数字类型的主键，并且只能配置一列。                                                                                                                                                                                |
| partition_lower_bound                      | BigDecimal | 否    | -               | 扫描时 `partition_column` 的最小值，如果未设置，`SeaTunnel` 将查询数据库以获取最小值。                                                                                                                                                            |
| partition_upper_bound                      | BigDecimal | 否    | -               | 扫描时 `partition_column` 的最大值，如果未设置，`SeaTunnel` 将查询数据库以获取最大值。                                                                                                                                                            |
| partition_num                              | Int        | 否    | 作业并行度 | 分区数量，仅支持正整数。<br/>默认值为作业并行度。                                                                                                                                                                                            |
| fetch_size                                 | Int        | 否    | 0               | 对于返回大量对象的查询，可以配置查询的行提取大小，以通过减少满足选择条件所需的数据库访问次数来提高性能。<br/>设置为零表示使用 `JDBC` 的默认值。                                                                                                                                         |
| properties                                 | Map        | 否    | -               | 额外的连接配置参数，当属性和 URL 中有相同的参数时，优先级由驱动程序的具体实现决定。<br/>例如，在 MySQL 中，属性优先于 URL。                                                                                                                                               |
| table_path                                 | String     | 否    | -               | 表的完整路径，您可以使用此配置代替 `query`。<br/>示例：<br/>mysql: "testdb.table1"<br/>oracle: "test_schema.table1"<br/>sqlserver: "testdb.test_schema.table1"<br/>postgresql: "testdb.test_schema.table1"                                  |
| table_list                                 | Array      | 否    | -               | 要读取的表的列表，您可以使用此配置代替 `table_path`，示例如下： ```[{ table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select * id, name from testdb.table2"}]```                                                         |
| where_condition                            | String     | 否    | -               | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`。                                                                                                                                                                     |
| split.size                                 | Int        | 否    | 8096            | 表的分割大小（行数），当读取表时，捕获的表会被分割成多个分片。                                                                                                                                                                                        |
| split.even-distribution.factor.lower-bound | Double     | 否    | 0.05            | 分片键分布因子的下限。该因子用于判断表数据的分布是否均匀。如果计算得到的分布因子大于或等于该下限（即，(MAX(id) - MIN(id) + 1) / 行数），则会对表的分片进行优化，以确保数据的均匀分布。反之，如果分布因子较低，则表数据将被视为分布不均匀。如果估算的分片数量超过 `sample-sharding.threshold` 所指定的值，则会采用基于采样的分片策略。默认值为 0.05。               |
| split.even-distribution.factor.upper-bound | Double     | 否    | 100             | 分片键分布因子的上限。该因子用于判断表数据的分布是否均匀。如果计算得到的分布因子小于或等于该上限（即，(MAX(id) - MIN(id) + 1) / 行数），则会对表的分片进行优化，以确保数据的均匀分布。反之，如果分布因子较大，则表数据将被视为分布不均匀，并且如果估算的分片数量超过 `sample-sharding.threshold` 所指定的值，则会采用基于采样的分片策略。默认值为 100.0。            |
| split.sample-sharding.threshold            | Int        | 否    | 10000           | 此配置指定了触发样本分片策略的估算分片数阈值。当分布因子超出由 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且估算的分片数量（计算方法为大致行数 / 分片大小）超过此阈值时，将使用样本分片策略。此配置有助于更高效地处理大型数据集。默认值为 1000 个分片。 |
| split.inverse-sampling.rate                | Int        | 否    | 1000            | 样本分片策略中使用的采样率的倒数。例如，如果该值设置为 1000，则表示在采样过程中应用 1/1000 的采样率。此选项提供了灵活性，可以控制采样的粒度，从而影响最终的分片数量。特别适用于处理非常大的数据集，在这种情况下通常会选择较低的采样率。默认值为 1000。                                                                                   |
| common-options                             |            | 否    | -               | 源插件的常见参数，请参阅 [源常见参数](../source-common-options.md) 了解详细信息。                                                                                                                                                              |

## 并行读取器

JDBC 源连接器支持从表中并行读取数据。SeaTunnel 将使用特定规则将表中的数据进行分割，然后将这些数据交给读取器进行读取。读取器的数量由 `parallelism` 选项决定。
**拆分键规则:**

1. 如果 `partition_column` 不为空，它将用于计算数据的分片。该列必须属于 **支持的分片数据类型**。
2. 如果 partition_column 为空，SeaTunnel 将从表中读取模式并获取主键和唯一索引。如果主键和唯一索引中有多个列，则会选择第一个属于 **支持的分片数据类型** 的列来进行数据分片。例如，如果表的主键是 `(nn guid, name varchar)`，因为 `guid` 不属于 **支持的分片数据类型**，所以会选择列 `name` 来进行数据分片。

**支持的拆分数据类型:**
* String
* Number(int, bigint, decimal, ...)
* Date

### 与拆分相关的参数

#### split.size

每个分片中的行数，捕获的表在读取时会被分成多个分片。

#### split.even-distribution.factor.lower-bound

> 不推荐使用

分片键分布因子的下限。该因子用于判断表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即，(最大(id) - 最小(id) + 1)/ 行数），则表的分片将被优化为均匀分布。否则，如果分布因子较小，则表的数据将被认为是不均匀分布的。如果估算的分片数量超过 `sample-sharding.threshold` 所指定的值，将使用基于采样的分片策略。默认值为 0.05。

#### split.even-distribution.factor.upper-bound

> 不推荐使用

分片键分布因子的上限。该因子用于判断表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即，(最大(id) - 最小(id) + 1）/ 行数)，则表的分片将被优化为均匀分布。否则，如果分布因子较大，则表的数据将被认为是不均匀分布的。如果估算的分片数量超过 `sample-sharding.threshold` 所指定的值，将使用基于采样的分片策略。默认值为 100.0。

#### split.sample-sharding.threshold

此配置指定了触发采样分片策略的估算分片数量阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且估算的分片数量（按大致行数除以分片大小计算）超过该阈值时，将使用采样分片策略。这有助于更高效地处理大数据集。默认值为 1000 个分片。

#### split.inverse-sampling.rate

采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了灵活性，可以控制采样的粒度，从而影响最终的分片数量。在处理非常大的数据集时，较低的采样率通常是首选。默认值为 1000。

#### partition_column [string]

拆分数据的列名称。

#### partition_upper_bound [BigDecimal]

扫描时 `partition_column` 的最大值。如果未设置，SeaTunnel 将查询数据库以获取最大值。

#### partition_lower_bound [BigDecimal]

扫描时 `partition_column` 的最小值。如果未设置，SeaTunnel 将查询数据库以获取最小值。

#### partition_num [int]

> 不推荐使用，正确的方法是通过 `split.size` 来控制分片的数量。

需要拆分成多少个分片，只支持正整数。默认值为作业并行度。

## 提示


> 如果表无法拆分（例如，表没有主键或唯一索引，且未设置 `partition_column`），则将以单线程并发方式运行。
>
> 使用 `table_path` 替代 `query` 来进行单表读取。如果需要读取多个表，请使用 `table_list`。

## 任务示例

### 简单的例子:

> 这个示例以单线程并行的方式查询测试数据库中 `type_bin` 为 'table' 的16条数据，并查询所有字段。你也可以指定查询哪些字段，并将最终结果输出到控制台。

```
# 定义运行时环境
env {
  parallelism = 4
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        query = "select * from type_bin limit 16"
    }
}

transform {
    # 如果您想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### 按 `partition_column` 并行

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        query = "select * from type_bin"
        partition_column = "id"
        split.size = 10000
        # Read start boundary
        #partition_lower_bound = ...
        # Read end boundary
        #partition_upper_bound = ...
    }
}

sink {
  Console {}
}
```

### 按主键或唯一索引并行

> 配置 `table_path` 将启用自动拆分，您可以配置 `split.*` 来调整拆分策略

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
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

### 并行边界：

> 指定数据的上下边界查询会更加高效。根据您配置的上下边界读取数据源会更高效。 

```
source {
    Jdbc {
        url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # Define query logic as required
        query = "select * from type_bin"
        partition_column = "id"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
        properties {
         useSSL=false
        }
    }
}
```

### 多表读取：

***配置 `table_list` 将启用自动拆分，您可以配置 `split.*` 来调整拆分策略***

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
    user = "root"
    password = "123456"

    table_list = [
      {
        table_path = "testdb.table1"
      },
      {
        table_path = "testdb.table2"
        # Use query filetr rows & columns
        query = "select id, name from testdb.table2 where id > 100"
      }
    ]
    #where_condition= "where id > 100"
    #split.size = 8096
    #split.even-distribution.factor.upper-bound = 100
    #split.even-distribution.factor.lower-bound = 0.05
    #split.sample-sharding.threshold = 1000
    #split.inverse-sampling.rate = 1000
  }
}

sink {
  Console {}
}
```

