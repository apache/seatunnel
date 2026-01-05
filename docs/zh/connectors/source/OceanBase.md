import ChangeLog from '../changelog/connector-jdbc.md';

# OceanBase

> JDBC OceanBase 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

通过 JDBC 读取外部数据源数据。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| OceanBase | 所有 OceanBase 服务器版本 | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [下载](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录<br/>
> 例如：cp oceanbase-client-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

### MySQL 模式

| MySQL 数据类型 | SeaTunnel 数据类型 |
|---------------|------------------|
| BIT(1)<br/>TINYINT(1) | BOOLEAN |
| TINYINT | BYTE |
| TINYINT<br/>TINYINT UNSIGNED | SMALLINT |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT | BIGINT |
| BIGINT UNSIGNED | DECIMAL(20,0) |
| DECIMAL(x,y)(<38) | DECIMAL(x,y) |
| DECIMAL(x,y)(>38) | DECIMAL(38,18) |
| DECIMAL UNSIGNED | DECIMAL |
| FLOAT<br/>FLOAT UNSIGNED | FLOAT |
| DOUBLE<br/>DOUBLE UNSIGNED | DOUBLE |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM | STRING |
| DATE | DATE |
| TIME | TIME |
| DATETIME<br/>TIMESTAMP | TIMESTAMP |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)<br/>GEOMETRY | BYTES |

### Oracle 模式

| Oracle 数据类型 | SeaTunnel 数据类型 |
|---------------|------------------|
| Integer | DECIMAL(38,0) |
| Number(p), p <= 9 | INT |
| Number(p), p <= 18 | BIGINT |
| Number(p), p > 18 | DECIMAL(38,18) |
| Number(p,s) | DECIMAL(p,s) |
| Float | DECIMAL(38,18) |
| REAL<br/> BINARY_FLOAT | FLOAT |
| BINARY_DOUBLE | DOUBLE |
| CHAR<br/>NCHAR<br/>VARCHAR<br/>VARCHAR2<br/>NVARCHAR2<br/>NCLOB<br/>CLOB<br/>LONG<br/>XML<br/>ROWID | STRING |
| DATE | TIMESTAMP |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE | TIMESTAMP |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE | BYTES |
| UNKNOWN | 暂不支持 |

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | JDBC 连接的 URL。参考示例：jdbc:oceanbase://localhost:2883/test |
| driver | String | 是 | - | 用于连接到远程数据源的 jdbc 类名，应为 `com.oceanbase.jdbc.Driver`。 |
| username | String | 否 | - | 连接实例用户名 |
| password | String | 否 | - | 连接实例密码 |
| compatible_mode | String | 是 | - | OceanBase 的兼容模式，可以是 'mysql' 或 'oracle'。 |
| query | String | 是 | - | 查询语句 |
| connection_check_timeout_sec | Int | 否 | 30 | 等待用于验证连接的数据库操作完成的时间（秒） |
| partition_column | String | 否 | - | 用于并行性分割的列名，仅支持数值类型列和字符串类型列。 |
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

```
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
  }
}

transform {
    # 如果您想了解有关如何配置 seatunnel 的更多信息并查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```

### 并行

> 使用您配置的分片字段和分片数据并行读取查询表。如果您想读取整个表，可以这样做

```
env {
  parallelism = 10
  job.mode = "BATCH"
}
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
    # 并行分片读取字段
    partition_column = "id"
    # 分片数量
    partition_num = 10
  }
}
sink {
  Console {}
}
```

### 并行边界

> 根据您配置的上下边界读取数据源更高效

```
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
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

