import ChangeLog from '../changelog/connector-jdbc.md';

# Vertica

> JDBC Vertica 源连接器

## 描述

通过 JDBC 读取外部数据源数据。

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://www.vertica.com/download/vertica/client-drivers/) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://www.vertica.com/download/vertica/client-drivers/) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL 并可以实现投影效果。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| Vertica | 不同的依赖版本有不同的驱动类 | com.vertica.jdbc.Driver | jdbc:vertica://localhost:5433/vertica | [下载](https://www.vertica.com/download/vertica/client-drivers/) |

## 数据类型映射

| Vertica 数据类型 | SeaTunnel 数据类型 |
|-----------------|------------------|
| BIT | BOOLEAN |
| TINYINT<br/>TINYINT UNSIGNED<br/>SMALLINT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT | LONG |
| BIGINT UNSIGNED | DECIMAL(20,0) |
| DECIMAL(x,y)(<38) | DECIMAL(x,y) |
| DECIMAL(x,y)(>38) | DECIMAL(38,18) |
| DECIMAL UNSIGNED | DECIMAL |
| FLOAT<br/>FLOAT UNSIGNED | FLOAT |
| DOUBLE<br/>DOUBLE UNSIGNED | DOUBLE |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON | STRING |
| DATE | DATE |
| TIME | TIME |
| DATETIME<br/>TIMESTAMP | TIMESTAMP |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n) | BYTES |
| GEOMETRY<br/>UNKNOWN | 暂不支持 |

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | JDBC 连接的 URL。参考示例：jdbc:vertica://localhost:5433/vertica |
| driver | String | 是 | - | 用于连接到远程数据源的 jdbc 类名，如果您使用 Vertica，值为 `com.vertica.jdbc.Driver`。 |
| username | String | 否 | - | 连接实例用户名 |
| password | String | 否 | - | 连接实例密码 |
| query | String | 是 | - | 查询语句 |
| connection_check_timeout_sec | Int | 否 | 30 | 等待用于验证连接的数据库操作完成的时间（秒） |
| partition_column | String | 否 | - | 用于并行性分割的列名，仅支持数值类型，仅支持数值类型主键，只能配置一列。 |
| partition_lower_bound | BigDecimal | 否 | - | partition_column 的最小值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最小值。 |
| partition_upper_bound | BigDecimal | 否 | - | partition_column 的最大值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最大值。 |
| partition_num | Int | 否 | job parallelism | 分割数量，仅支持正整数。默认值是任务并行度。 |
| fetch_size | Int | 否 | 0 | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，以通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。 |
| properties | Map | 否 | - | 其他连接配置参数，当 properties 和 URL 具有相同参数时，优先级由驱动程序的具体实现确定。例如，在 MySQL 中，properties 优先于 URL。 |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。 |

### 提示

> 如果未设置 partition_column，它将以单并发运行，如果设置了 partition_column，它将根据任务的并发度并行执行。

## 任务示例

### 简单

> 此示例在单个并行中查询您的测试"数据库"中的 type_bin 表的 16 条数据，并查询其所有字段。您也可以指定要查询的字段以最终输出到控制台。

```
# 定义运行时环境
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:vertica://localhost:5433/vertica"
        driver = "com.vertica.jdbc.Driver"
        connection_check_timeout_sec = 100
        username = "root"
        password = "123456"
        query = "select * from type_bin limit 16"
    }
}

transform {
    # 如果您想了解有关如何配置 seatunnel 的更多信息并查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/transform-v2/sql
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
        url = "jdbc:vertica://localhost:5433/vertica"
        driver = "com.vertica.jdbc.Driver"
        connection_check_timeout_sec = 100
        username = "root"
        password = "123456"
        # 根据需要定义查询逻辑
        query = "select * from type_bin"
        # 并行分片读取字段
        partition_column = "id"
        # 分片数量
        partition_num = 10
    }
}
```

### 并行边界

> 指定查询的上下边界内的数据更高效。根据您配置的上下边界读取数据源更高效

```
source {
    Jdbc {
        url = "jdbc:vertica://localhost:5433/vertica"
        driver = "com.vertica.jdbc.Driver"
        connection_check_timeout_sec = 100
        username = "root"
        password = "123456"
        # 根据需要定义查询逻辑
        query = "select * from type_bin"
        partition_column = "id"
        # 读取开始边界
        partition_lower_bound = 1
        # 读取结束边界
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

## 变更日志

<ChangeLog />

