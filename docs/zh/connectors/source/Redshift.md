import ChangeLog from '../changelog/connector-jdbc.md';

# Redshift

> JDBC Redshift 源连接器

## 描述

通过 JDBC 读取外部数据源数据。

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL 并可以实现投影效果。

## 支持的数据源列表

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| redshift | 不同的依赖版本有不同的驱动类 | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [下载](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

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

