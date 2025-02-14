# PostgreSQL

> JDBC PostgreSQL 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动的jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [严格一次性](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../concept/connector-v2-features.md)

> 支持查询 SQL，并可以实现投影效果。

## 描述

通过 JDBC 读取外部数据源数据。

## 支持的数据源信息

| 数据源         |                     支持的版本                      |        驱动         |                  URL                  |                                  Maven                                   |
|----------------|----------------------------------------------------|---------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL     | 不同的依赖版本有不同的驱动类。                      | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql)     |
| PostgreSQL     | 如果您想在 PostgreSQL 中操作 GEOMETRY 类型。      | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)     |

## 数据库依赖

> 请下载与 'Maven' 对应的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录中<br/>
> 例如，对于 PostgreSQL 数据源： cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/<br/>
> 如果您想在 PostgreSQL 中操作 GEOMETRY 类型，请将 postgresql-xxx.jar 和 postgis-jdbc-xxx.jar 添加到 $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|                                       PostgreSQL 数据类型                                       |                                                               SeaTunnel 数据类型                                                               |
|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                                                        | BOOLEAN                                                                                                                                        |
| _BOOL<br/>                                                                                       | ARRAY&LT;BOOLEAN&GT;                                                                                                                           |
| BYTEA<br/>                                                                                       | BYTES                                                                                                                                          |
| _BYTEA<br/>                                                                                      | ARRAY&LT;TINYINT&GT;                                                                                                                           |
| INT2<br/>SMALLSERIAL                                                                             | SMALLINT                                                                                                                                       |
| _INT2                                                                                            | ARRAY&LT;SMALLINT&GT;                                                                                                                          |
| INT4<br/>SERIAL<br/>                                                                             | INT                                                                                                                                            |
| _INT4<br/>                                                                                       | ARRAY&LT;INT&GT;                                                                                                                               |
| INT8<br/>BIGSERIAL<br/>                                                                          | BIGINT                                                                                                                                         |
| _INT8<br/>                                                                                       | ARRAY&LT;BIGINT&GT;                                                                                                                            |
| FLOAT4<br/>                                                                                      | FLOAT                                                                                                                                          |
| _FLOAT4<br/>                                                                                     | ARRAY&LT;FLOAT&GT;                                                                                                                             |
| FLOAT8<br/>                                                                                      | DOUBLE                                                                                                                                         |
| _FLOAT8<br/>                                                                                     | ARRAY&LT;DOUBLE&GT;                                                                                                                            |
| NUMERIC(指定列的列大小>0)                                                                         | DECIMAL(指定列的列大小，获取指定列小数点右侧的数字位数)                                                                                            |
| NUMERIC(指定列的列大小<0)                                                                         | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB<br/>UUID | STRING                                                                                                                                         |
| _BPCHAR<br/>_CHARACTER<br/>_VARCHAR<br/>_TEXT                                                    | ARRAY&LT;STRING&GT;                                                                                                                            |
| TIMESTAMP(s)<br/>TIMESTAMPTZ(s)                                                                  | TIMESTAMP(s)                                                                                                                                   |
| TIME(s)<br/>TIMETZ(s)                                                                            | TIME(s)                                                                                                                                        |
| DATE<br/>                                                                                        | DATE                                                                                                                                           |

## 选项

|                    名称                     | 类型         | 必需 |     默认     |                                                                                                                                                                                                                                                                                                     描述                                                                                                                                                                                                                                                                                                      |
|----------------------------------------------|------------|------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                          | String        | 是   | -               | JDBC 连接的 URL。参考示例：jdbc:postgresql://localhost:5432/test                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| driver                                       | String        | 是   | -               | 用于连接到远程数据源的 JDBC 类名，<br/> 如果您使用 MySQL，则值为 `com.mysql.cj.jdbc.Driver`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| user                                         | String        | 否   | -               | 连接实例的用户名                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| password                                     | String        | 否   | -               | 连接实例的密码                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| query                                        | String        | 是   | -               | 查询语句                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| connection_check_timeout_sec                 | Int         | 否   | 30              | 用于验证连接的数据库操作完成的等待时间（秒）                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_column                             | String        | 否   | -               | 用于并行化的分区列名，仅支持数字类型，<br/> 仅支持数字类型主键，并且只能配置一列。                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| partition_lower_bound                        | BigDecimal | 否   | -               | 扫描的 partition_column 的最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| partition_upper_bound                        | BigDecimal | 否   | -               | 扫描的 partition_column 的最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| partition_num                                | Int         | 否   | 作业并行性      | 分区数量，仅支持正整数。默认值为作业并行性                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| fetch_size                                   | Int         | 否   | 0               | 对于返回大量对象的查询，您可以配置<br/> 用于查询的行抓取大小，以通过减少所需的数据库访问次数来提高性能。<br/> 0 表示使用 JDBC 默认值。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| properties                                   | Map        | 否   | -               | 其他连接配置参数，当属性和 URL 具有相同参数时，<br/> 优先级由驱动程序的具体实现决定。在 MySQL 中，属性优先于 URL。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| table_path                                   | String        | 否   | -               | 表的完整路径，您可以使用此配置替代 `query`。<br/> 示例：<br/> mysql: "testdb.table1" <br/> oracle: "test_schema.table1" <br/> sqlserver: "testdb.test_schema.table1" <br/> postgresql: "testdb.test_schema.table1"                                                                                                                                                                                                                                                                                                                                                         |
| table_list                                   | Array         | 否   | -               | 要读取的表列表，您可以使用此配置替代 `table_path` 示例：```[{ table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select * id, name from testdb.table2"}]```                                                                                                                                                                                                                                                                                                                                                                                               |
| where_condition                              | String        | 否   | -               | 所有表/查询的通用行过滤条件，必须以 `where` 开头。 例如 `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| split.size                                   | Int         | 否   | 8096            | 表的拆分大小（行数），被捕获的表在读取时被拆分为多个拆分。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.even-distribution.factor.lower-bound   | Double        | 否   | 0.05            | 块键分布因子的下限。此因子用于确定表数据是否均匀分布。<br/> 如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），则表块将优化为均匀分布。否则，如果分布因子较小，则将视为不均匀分布，当估计的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 0.05。  |
| split.even-distribution.factor.upper-bound   | Double        | 否   | 100             | 块键分布因子的上限。此因子用于确定表数据是否均匀分布。<br/> 如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），则表块将优化为均匀分布。否则，如果分布因子较大，则将视为不均匀分布，当估计的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 100.0。 |
| split.sample-sharding.threshold              | Int         | 否   | 10000           | 此配置指定触发样本分片策略的估计分片数阈值。<br/> 当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围时，且估计的分片数（计算为近似行数 / 块大小）超过此阈值，将使用样本分片策略。这可以帮助更高效地处理大数据集。默认值为 1000 个分片。                                                                                   |
| split.inverse-sampling.rate                  | Int         | 否   | 1000            | 在样本分片策略中使用的采样率的逆数。例如，如果此值设置为 1000，表示在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理非常大的数据集时，较低的采样率尤其有用。默认值为 1000。                                                                                                                                                              |
|
## 并行读取器

JDBC 源连接器支持从表中并行读取数据。SeaTunnel 将使用某些规则来拆分表中的数据，这些数据将交给读取器进行读取。读取器的数量由 `parallelism` 选项确定。

**拆分键规则：**

1. 如果 `partition_column` 不为 null，将用于计算拆分。该列必须属于 **支持的拆分数据类型**。
2. 如果 `partition_column` 为 null，SeaTunnel 将从表中读取模式并获取主键和唯一索引。如果主键和唯一索引中有多列，则使用第一个属于 **支持的拆分数据类型** 的列来拆分数据。例如，表有主键(nn guid, name varchar)，因为 `guid` 不在 **支持的拆分数据类型** 中，因此将使用列 `name` 来拆分数据。

**支持的拆分数据类型：**
* 字符串
* 数字（int, bigint, decimal, ...）
* 日期

### 与拆分相关的选项

#### split.size

每个拆分中有多少行，当读取表时，被捕获的表将拆分为多个拆分。

#### split.even-distribution.factor.lower-bound

> 不推荐使用

块键分布因子的下限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），则表块将优化为均匀分布。否则，如果分布因子较小，则将视为不均匀分布，当估计的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 0.05。

#### split.even-distribution.factor.upper-bound

> 不推荐使用

块键分布因子的上限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），则表块将优化为均匀分布。否则，如果分布因子较大，则将视为不均匀分布，当估计的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 100.0。

#### split.sample-sharding.threshold

此配置指定触发样本分片策略的估计分片数阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围时，且估计的分片数（计算为近似行数 / 块大小）超过此阈值，将使用样本分片策略。这可以帮助更高效地处理大数据集。默认值为 1000 个分片。

#### split.inverse-sampling.rate

在样本分片策略中使用的采样率的逆数。例如，如果此值设置为 1000，表示在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理非常大的数据集时，较低的采样率尤其有用。默认值为 1000。

#### partition_column [字符串]

用于拆分数据的列名。

#### partition_upper_bound [BigDecimal]

扫描的 partition_column 最大值，如果未设置，SeaTunnel 将查询数据库获取最大值。

#### partition_lower_bound [BigDecimal]

扫描的 partition_column 最小值，如果未设置，SeaTunnel 将查询数据库获取最小值。

#### partition_num [整数]

> 不推荐使用，正确的方法是通过 `split.size` 控制拆分数量

我们需要拆分成多少个拆分，仅支持正整数。默认值为作业并行性。

## 提示

> 如果表无法拆分（例如，表没有主键或唯一索引，并且未设置 `partition_column`），将以单一并发运行。
>
> 使用 `table_path` 替代 `query` 进行单表读取。如果需要读取多个表，请使用 `table_list`。

## 任务示例

### 简单示例：

> 此示例查询您测试 "database" 中 type_bin 为 'table' 的 16 条数据，并以单并行方式查询其所有字段。您还可以指定要查询的字段，以便最终输出到控制台。

```
# Defining the runtime environment
env {
  parallelism = 4
  job.mode = "BATCH"
}

source{
    Jdbc {
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "test"
        query = "select * from source limit 16"
    }
}

transform {
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### 按 partition_column 并行读取

> 使用您配置的分片字段和分片数据并行读取查询表。如果您想要读取整个表，可以这样做。

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source{
    jdbc{
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "test"
        query = "select * from source"
        partition_column= "id"
        partition_num = 5
    }
}
sink {
  Console {}
}
```

### 按主键或唯一索引并行读取

> 配置 `table_path` 将启用自动拆分，您可以配置 `split.*` 来调整拆分策略。

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        table_path = "test.public.AllDataType_1"
        query = "select * from public.AllDataType_1"
        split.size = 10000
    }
}

sink {
  Console {}
}
```

### 并行边界：

> 在查询中指定上下边界内的数据更为高效。根据您配置的上下边界读取数据源将更为高效。

```
source{
    jdbc{
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "test"
        query = "select * from source"
        partition_column= "id"
        
        # The name of the table returned
        plugin_output = "jdbc"
        partition_lower_bound = 1
        partition_upper_bound = 50
        partition_num = 5
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
    url="jdbc:postgresql://datasource01:5432/demo"
    user="iDm82k6Q0Tq+wUprWnPsLQ=="
    driver="org.postgresql.Driver"
    password="iDm82k6Q0Tq+wUprWnPsLQ=="
    "table_list"=[
        {
            "table_path"="demo.public.AllDataType_1"
        },
        {
            "table_path"="demo.public.alldatatype"
        }
    ]
    #where_condition= "where id > 100"
    split.size = 10000
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

