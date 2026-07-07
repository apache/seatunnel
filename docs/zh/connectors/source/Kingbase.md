import ChangeLog from '../changelog/connector-jdbc.md';

# Kingbase

> JDBC Kingbase 源连接器

## 支持连接器版本

- 8.6

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

通过 JDBC 读取外部数据源数据。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| Kingbase | 8.6 | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [下载](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录<br/>
> 例如：cp kingbase8-8.6.0.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

| Kingbase 数据类型 | SeaTunnel 数据类型 |
|------------------|------------------|
| BOOL | BOOLEAN |
| INT2 | SHORT |
| SMALLSERIAL <br/>SERIAL <br/>INT4 | INT |
| INT8 <br/>BIGSERIAL | BIGINT |
| FLOAT4 | FLOAT |
| FLOAT8 | DOUBLE |
| NUMERIC | DECIMAL |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT | STRING |
| TIMESTAMP | LOCALDATETIME |
| TIME | LOCALTIME |
| DATE | LOCALDATE |
| 其他数据类型 | 暂不支持 |

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | JDBC 连接的 URL。参考示例：jdbc:kingbase8://localhost:54321/test |
| driver | String | 是 | - | 用于连接到远程数据源的 jdbc 类名，应为 `com.kingbase8.Driver`。 |
| username | String | 否 | - | 连接实例用户名 |
| password | String | 否 | - | 连接实例密码 |
| query | String | 是 | - | 查询语句 |
| connection_check_timeout_sec | Int | 否 | 30 | 等待用于验证连接的数据库操作完成的时间（秒） |
| partition_column | String | 否 | - | 用于并行性分割的列名，仅支持数值类型列和字符串类型列。 |
| partition_lower_bound | BigDecimal | 否 | - | partition_column 的最小值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最小值。 |
| partition_upper_bound | BigDecimal | 否 | - | partition_column 的最大值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最大值。 |
| partition_num | Int | 否 | job parallelism | 分割数量，仅支持正整数。默认值是任务并行度。 |
| fetch_size | Int | 否 | 0 | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，以通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。 |
| use_regex                                  | Boolean    | 否    | false | 控制表路径的正则表达式匹配。当设置为true时，table_path 将被视为正则表达式模式。当设置为false或未指定时，table_path 将被视为精确路径（不进行正则匹配）。                                                                                                                            |
| table_path                                 | String     | 否    | -     | 表的完整路径，您可以使用此配置代替 `query`。<br/>示例：<br/>"testdb.table1"                                  |
| table_list                                 | Array      | 否    | -     | 要读取的表的列表，您可以使用此配置代替 `table_path`，示例如下： ```[{ table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select * id, name from testdb.table2"}]```                                                         |
| where_condition                            | String     | 否    | -     | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`。                                                                                                                                                                     |
| split.size                                 | Int        | 否    | 8096  | 表的分割大小（行数），当读取表时，捕获的表会被分割成多个分片。                                                                                                                                                                                        |
| split.even-distribution.factor.lower-bound | Double     | 否    | 0.05  | 分片键分布因子的下限。该因子用于判断表数据的分布是否均匀。如果计算得到的分布因子大于或等于该下限（即，(MAX(id) - MIN(id) + 1) / 行数），则会对表的分片进行优化，以确保数据的均匀分布。反之，如果分布因子较低，则表数据将被视为分布不均匀。如果估算的分片数量超过 `sample-sharding.threshold` 所指定的值，则会采用基于采样的分片策略。默认值为 0.05。               |
| split.even-distribution.factor.upper-bound | Double     | 否    | 100   | 分片键分布因子的上限。该因子用于判断表数据的分布是否均匀。如果计算得到的分布因子小于或等于该上限（即，(MAX(id) - MIN(id) + 1) / 行数），则会对表的分片进行优化，以确保数据的均匀分布。反之，如果分布因子较大，则表数据将被视为分布不均匀，并且如果估算的分片数量超过 `sample-sharding.threshold` 所指定的值，则会采用基于采样的分片策略。默认值为 100.0。            |
| split.sample-sharding.threshold            | Int        | 否    | 10000 | 此配置指定了触发样本分片策略的估算分片数阈值。当分布因子超出由 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且估算的分片数量（计算方法为大致行数 / 分片大小）超过此阈值时，将使用样本分片策略。此配置有助于更高效地处理大型数据集。默认值为 1000 个分片。 |
| split.inverse-sampling.rate                | Int        | 否    | 1000  | 样本分片策略中使用的采样率的倒数。例如，如果该值设置为 1000，则表示在采样过程中应用 1/1000 的采样率。此选项提供了灵活性，可以控制采样的粒度，从而影响最终的分片数量。特别适用于处理非常大的数据集，在这种情况下通常会选择较低的采样率。默认值为 1000。                                                                                   |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。 |

### 提示

> 如果未设置 partition_column，它将以单并发运行，如果设置了 partition_column，它将根据任务的并发度并行执行。

## 任务示例

### 简单

```
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
  }
}

transform {
    # 如果您想了解有关如何配置 seatunnel 的更多信息并查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/transforms/sql
}

sink {
    Console {}
}
```

### 并行

> 使用您配置的分片字段和分片数据并行读取查询表。如果您想读取整个表，可以这样做

```
source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
    # 并行分片读取字段
    partition_column = "id"
    # 分片数量
    partition_num = 10
  }
}
```

### 并行边界

> 根据您配置的上下边界读取数据源更高效

```
source {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/db_test"
    username = "root"
    password = ""
    query = "select * from source"
    partition_column = "id"
    partition_num = 10
    # 读取开始边界
    partition_lower_bound = 1
    # 读取结束边界
    partition_upper_bound = 500
  }
}
```

## 变更日志

<ChangeLog />

