import ChangeLog from '../changelog/connector-jdbc.md';

# Redshift

> JDBC Redshift 源连接器

## 描述

通过 JDBC 接口从 Amazon Redshift 读取数据。连接器使用 Redshift JDBC 驱动
（`com.amazon.redshift.jdbc.Driver`），提交配置的 `query` 获取行结果。
Redshift 兼容 PostgreSQL，因此连接器可以读取 JDBC 用户有权访问的所有表，
包括列式 `SUPER` 字段以及标准标量类型。支持通过 `partition_column` 进行并行
读取，也支持通过 `table_list` 进行多表读取。

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL 并可以实现投影效果。

## 数据源依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 支持的数据源列表

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| redshift | 不同的依赖版本有不同的驱动类 | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [下载](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## 源选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| url | String | 是 | - | JDBC 连接 URL，例如 `jdbc:redshift://localhost:5439/database`。 |
| driver | String | 是 | - | 用于连接 Redshift 的 JDBC 类名，固定为 `com.amazon.redshift.jdbc.Driver`。 |
| username | String | 否 | - | 连接实例的用户名。 |
| password | String | 否 | - | 连接实例的密码。 |
| query | String | 否 | - | SELECT 语句。与 `table_path`/`table_list` 配合使用，查询的列列表决定输出 schema。 |
| table_path | String | 否 | - | 要读取的完整表名，例如 `public.table2`。单表读取时可直接使用。 |
| table_list | Array | 否 | - | 要读取的表列表，每一项可以覆盖 `table_path` 和 `query`。用于开启多表读取与自动分片。 |
| connection_check_timeout_sec | Int | 否 | 30 | 用于校验连接的数据库操作超时时间，单位秒。 |
| partition_column | String | 否 | - | 用于并行读取的拆分列，仅支持数值类型的主键列。 |
| partition_lower_bound | BigDecimal | 否 | - | `partition_column` 的最小扫描值。未配置时连接器从数据库中查询最小值。 |
| partition_upper_bound | BigDecimal | 否 | - | `partition_column` 的最大扫描值。未配置时连接器从数据库中查询最大值。 |
| partition_num | Int | 否 | 作业并行度 | 分片数量，仅支持正整数，默认与作业并行度相同。 |
| fetch_size | Int | 否 | 0 | 大结果集查询时的 JDBC 拉取行数，`0` 表示使用驱动默认。 |
| where_condition | String | 否 | - | 对所有表/查询生效的统一行过滤条件，必须以 `where` 开头，例如 `where id > 100`。 |
| split.size | Int | 否 | 8096 | 使用 `table_path` 或 `table_list` 时自动分片的行数。 |
| common-options |  | 否 | - | 源插件通用参数，详情请参考 [Source 通用选项](../common-options/source-common-options.md)。 |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录<br/>
> 例如 Redshift 数据源：cp RedshiftJDBC42-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

| Redshift 数据类型 | SeaTunnel 数据类型 |
|------------------|------------------|
| SMALLINT<br />INT2 | SHORT |
| INTEGER<br />INT<br />INT4 | INT |
| BIGINT<br />INT8<br />OID | LONG |
| DECIMAL<br />NUMERIC | DECIMAL |
| REAL<br />FLOAT4 | FLOAT |
| DOUBLE_PRECISION<br />FLOAT8<br />FLOAT | DOUBLE |
| BOOLEAN<br />BOOL | BOOLEAN |
| CHAR<br />CHARACTER<br />NCHAR<br />BPCHAR<br />VARCHAR<br />CHARACTER_VARYING<br />NVARCHAR<br />TEXT<br />SUPER | STRING |
| VARBYTE<br />BINARY_VARYING | BYTES |
| TIME<br />TIME_WITH_TIME_ZONE<br />TIMETZ | LOCALTIME |
| TIMESTAMP<br />TIMESTAMP_WITH_OUT_TIME_ZONE<br />TIMESTAMPTZ | LOCALDATETIME |

## 示例

### 简单

> 此示例在单个并行中查询您的测试"数据库"中的 type_bin 表的 16 条数据，并查询其所有字段。您也可以指定要查询的字段以最终输出到控制台。

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:redshift://localhost:5439/dev"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "root"
        password = "123456"
        
        table_path = "public.table2"
        # 使用查询过滤行和列
        query = "select id, name from public.table2 where id > 100"
        
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

### 多表读取

***配置 `table_list` 将打开自动分割，您可以配置 `split.*` 来调整分割策略***

```hocon
env {
  job.mode = "BATCH"
  parallelism = 2
}
source {
  Jdbc {
    url = "jdbc:redshift://localhost:5439/dev"
    driver = "com.amazon.redshift.jdbc.Driver"
    username = "root"
    password = "123456"

    table_list = [
      {
        table_path = "public.table1"
      },
      {
        table_path = "public.table2"
        # 使用查询过滤行和列
        query = "select id, name from public.table2 where id > 100"
      }
    ]
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

## 变更日志

<ChangeLog />

