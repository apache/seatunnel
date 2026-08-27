import ChangeLog from '../changelog/connector-jdbc.md';

# Oracle

> JDBC Oracle 源连接器

## 描述

通过 JDBC 读取外部数据源数据。

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
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL 并可以实现投影效果。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| Oracle | 不同的依赖版本有不同的驱动类 | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## 数据库依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。
> 2. 要支持 i18n 字符集，请将 `orai18n.jar` 复制到 `$SEATUNNEL_HOME/plugins/` 目录。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。
> 2. 要支持 i18n 字符集，请将 `orai18n.jar` 复制到 `$SEATUNNEL_HOME/lib/` 目录。

## 数据类型映射

| Oracle 数据类型 | SeaTunnel 数据类型 |
|-----------------|------------------|
| INTEGER | DECIMAL(38,0) |
| FLOAT | DECIMAL(38, 18) |
| NUMBER(precision <= 9, scale == 0) | INT |
| NUMBER(9 < precision <= 18, scale == 0) | BIGINT |
| NUMBER(18 < precision, scale == 0) | DECIMAL(38, 0) |
| NUMBER(scale != 0) | DECIMAL(38, 18) |
| BINARY_DOUBLE | DOUBLE |
| BINARY_FLOAT<br/>REAL | FLOAT |
| CHAR<br/>NCHAR<br/>VARCHAR<br/>NVARCHAR2<br/>VARCHAR2<br/>LONG<br/>ROWID<br/>NCLOB<br/>CLOB<br/>XML<br/>INTERVAL | STRING |
| DATE | TIMESTAMP |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE | TIMESTAMP |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE | BYTES |

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | JDBC 连接的 URL。参考示例：jdbc:oracle:thin:@datasource01:1523:xe |
| driver | String | 是 | - | 用于连接到远程数据源的 jdbc 类名，如果您使用 Oracle，值为 `oracle.jdbc.OracleDriver`。 |
| username | String | 否 | - | 连接实例用户名 |
| password | String | 否 | - | 连接实例密码 |
| query | String | 否 | - | 查询语句。当未配置 `table_path` 和 `table_list` 时必填。 |
| connection_check_timeout_sec | Int | 否 | 30 | 等待用于验证连接的数据库操作完成的时间（秒） |
| partition_column | String | 否 | - | 用于并行性分割的列名，仅支持数值类型，仅支持数值类型主键，只能配置一列。 |
| partition_lower_bound | BigDecimal | 否 | - | partition_column 的最小值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最小值。 |
| partition_upper_bound | BigDecimal | 否 | - | partition_column 的最大值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最大值。 |
| partition_num | Int | 否 | job parallelism | 分割数量，仅支持正整数。默认值是任务并行度。 |
| fetch_size | Int | 否 | 0 | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，以通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。 |
| properties | Map | 否 | - | 其他连接配置参数，当 properties 和 URL 具有相同参数时，优先级由驱动程序的具体实现确定。例如，在 Oracle 中，properties 优先于 URL。 |
| use_regex | Boolean | 否 | false | 控制 table_path 的正则表达式匹配。设置为 `true` 时，table_path 将被视为正则表达式模式。设置为 `false` 或未指定时，table_path 将被视为精确路径（无正则表达式匹配）。 |
| table_path | String | 否 | - | 表的完整路径，您可以使用此配置代替 `query`。<br/>示例：<br/>"test_schema.table1" |
| table_list | Array | 否 | - | 要读取的表列表，您可以使用此配置代替 `table_path`。示例：```[{ table_path = "SCHEMA1.TABLE1"}, {table_path = "SCHEMA1.TABLE2", query = "select ID, NAME from SCHEMA1.TABLE2"}]``` |
| where_condition | String | 否 | - | 所有表/查询的通用行过滤条件，必须以 `where` 开头。 |
| split.size | Int | 否 | 8096 | 一个分割中有多少行。 |
| split.even-distribution.factor.lower-bound | Double | 否 | 0.05 | 分片键分布因子的下限，用于判断数据是否均匀分布。 |
| split.even-distribution.factor.upper-bound | Double | 否 | 100 | 分片键分布因子的上限，用于判断数据是否均匀分布。 |
| split.sample-sharding.threshold | Int | 否 | 1000 | 触发采样分片策略的估算分片数阈值。 |
| split.inverse-sampling.rate | Int | 否 | 1000 | 采样分片策略使用的采样率倒数，例如 `1000` 表示 1/1000 采样。 |
| split.allow-sampling | Boolean | 否 | true | 是否允许对分布不均匀的分片键使用采样分片策略。设置为 `false` 时，SeaTunnel 会退回到迭代式不均匀分片。 |
| use_select_count | Boolean | 否 | false | 是否使用 `select count(*)` 在分片前估算表行数。 |
| skip_analyze | Boolean | 否 | false | 是否跳过分片前的表行数分析。 |
| decimal_type_narrowing | Boolean | 否 | true | Decimal 类型收窄。为 `true` 时，如果没有精度损失，会把 Oracle Decimal 类型收窄为 Int 或 Long。 |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。 |

### decimal_type_narrowing

`decimal_type_narrowing` 为 `true` 时，如果没有精度损失，会把 Oracle Decimal 类型收窄为 Int 或 Long。当前仅在 Oracle 上生效。

`decimal_type_narrowing = true` 时：

| Oracle | SeaTunnel |
|---|---|
| NUMBER(1, 0) | Boolean |
| NUMBER(6, 0) | INT |
| NUMBER(10, 0) | BIGINT |

`decimal_type_narrowing = false` 时：

| Oracle | SeaTunnel |
|---|---|
| NUMBER(1, 0) | Decimal(1, 0) |
| NUMBER(6, 0) | Decimal(6, 0) |
| NUMBER(10, 0) | Decimal(10, 0) |

## 并行读取

JDBC Source 连接器支持并行读取表数据。SeaTunnel 会按一定规则把表数据拆分为多个分片，交给 Reader 并行读取，Reader 数量由 `parallelism` 选项决定。

**分片键规则：**

1. 如果设置了 `partition_column`，则用该列计算分片。该列必须在 **支持的分片数据类型** 中。
2. 如果 `partition_column` 未设置，SeaTunnel 会读取表结构并取主键和唯一索引。如果主键或唯一索引由多个列组成，则取第一个落在 **支持的分片数据类型** 中的列。例如表的主键是 `(guid, name varchar)`，由于 `guid` 不在支持的分片类型中，会使用 `name` 列进行分片。

**支持的分片数据类型：**
- String
- Number（int、bigint、decimal 等）
- Date

### 与分片相关的选项

#### split.size

一个分片包含多少行；读取表时，捕获的表会按 `split.size` 拆分为多个分片。

#### split.even-distribution.factor.lower-bound

> 不推荐修改

分片键分布因子的下限。该因子用于判断表数据是否均匀分布。如果计算得到的分布因子大于等于该下限（即 `(MAX(id) - MIN(id) + 1) / 行数`），则按均匀分布拆分；否则视为分布不均匀，并在估算分片数超过 `split.sample-sharding.threshold` 时使用采样分片策略。默认 `0.05`。

#### split.even-distribution.factor.upper-bound

> 不推荐修改

分片键分布因子的上限。如果分布因子小于等于该上限，按均匀分布拆分；否则视为不均匀分布，使用采样分片策略。默认 `100.0`。

#### split.sample-sharding.threshold

触发采样分片策略的估算分片数阈值。当分布因子超出 `split.even-distribution.factor.upper-bound` 和 `split.even-distribution.factor.lower-bound` 区间，并且估算分片数（行数 / `split.size`）超过该阈值时，会使用采样分片策略。默认 `1000`。

#### split.inverse-sampling.rate

采样分片策略的采样率倒数。例如 `1000` 表示 1/1000 采样率。默认 `1000`。

#### partition_column [string]

用于分片的列名。

#### partition_upper_bound [BigDecimal]

`partition_column` 的扫描最大值。未设置时 SeaTunnel 会查询数据库获取。

#### partition_lower_bound [BigDecimal]

`partition_column` 的扫描最小值。未设置时 SeaTunnel 会查询数据库获取。

#### partition_num [int]

> 不推荐修改，推荐使用 `split.size` 控制分片大小。

将数据拆分为多少个分片，仅支持正整数。默认等于任务并行度。

## 提示

> 如果表无法拆分（例如表没有主键或唯一索引，且未设置 `partition_column`），将以单并发运行。
>
> 单表读取可使用 `table_path` 代替 `query`。需要读取多张表时，请使用 `table_list`。

## 任务示例

### 简单示例

> 该示例从你 Oracle 中的 test 数据库查询名为 TEST_TABLE 的 16 条数据，以单并行方式运行，并查询其所有字段。你也可以指定要查询的字段，最终输出到控制台。

```
# 定义运行时环境
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    username = "root"
    password = "123456"
    query = "SELECT * FROM TEST_TABLE"
  }
}

transform {
  # 更多 transform 插件配置请参考 https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

### 按 partition_column 并行

> 通过配置分片字段和分片数据，可以并行读取查询表中的数据。需要读取整张表时可以使用这种方式。

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    # 按需定义查询逻辑
    query = "SELECT * FROM TEST_TABLE"
    # 用于并行分片读取的字段
    partition_column = "ID"
    # 分片数量
    partition_num = 10
    properties {
      database.oracle.jdbc.timezoneAsRegion = "false"
    }
  }
}
sink {
  Console {}
}
```

### 按主键或唯一索引并行

> 配置 `table_path` 会开启自动分片，可通过 `split.*` 选项调整分片策略。

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    table_path = "DA.SCHEMA1.TABLE1"
    query = "select * from SCHEMA1.TABLE1"
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### 并行边界

> 显式指定查询的上下界可以更高效地读取数据源。

```
source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    # 按需定义查询逻辑
    query = "SELECT * FROM TEST_TABLE"
    partition_column = "ID"
    # 读取起点
    partition_lower_bound = 1
    # 读取终点
    partition_upper_bound = 500
    partition_num = 10
  }
}
```

### 多表读取

***配置 `table_list` 会开启自动分片，可通过 `split.*` 选项调整分片策略***

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}
source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    connection_check_timeout_sec = 100
    username = "root"
    password = "123456"
    "table_list" = [
      {
        "table_path" = "XE.TEST.USER_INFO"
      },
      {
        "table_path" = "XE.TEST.YOURTABLENAME"
      }
    ]
    #where_condition = "where id > 100"
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

### 流式增量 ID 区间读取

Oracle Source 本质上是一个批连接器。设置 `job.mode = "STREAMING"` 只用于开启 checkpoint 以便在失败时恢复作业；source 本身仍然是有界的，每次作业只会读取一次配置好的 `[partition_lower_bound, partition_upper_bound)` 区间。如需周期性地拉取新增数据，必须在外部重新提交作业（例如按计划滑动区间窗口），或改用 Oracle-CDC 做持续变更捕获。

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    username = "root"
    password = "123456"
    query = "SELECT * FROM ORDERS WHERE ORDER_ID >= ? AND ORDER_ID < ?"
    partition_column = "ORDER_ID"
    partition_lower_bound = 1
    partition_upper_bound = 1000000
    partition_num = 16
  }
}
```

### 使用 TNS 连接串

如果 Oracle 部署只暴露 TNS 别名，可以把 `url` 指向 TNS 别名。TNS 名称由 classpath 上的 `oracle.net.tns_admin` 解析。

```hocon
source {
  Jdbc {
    url = "jdbc:oracle:thin:@tns_alias"
    driver = "oracle.jdbc.OracleDriver"
    username = "root"
    password = "123456"
    properties {
      oracle.net.tns_admin = "/etc/oracle"
    }
    table_path = "SCHEMA.ORDERS"
    split.size = 10000
  }
}
```

### 使用 where_condition 过滤行

通过 `where_condition` 可以为 `table_list` 或 `query` 中的所有条目应用一个公共过滤条件。字符串必须以 `where` 开头，以便拼接到自定义查询或 `table_path` 自动生成的查询后。

```hocon
source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    username = "root"
    password = "123456"
    table_path = "SCHEMA.ORDERS"
    where_condition = "where status = 'ACTIVE' and created_at >= DATE '2026-01-01'"
    split.size = 10000
  }
}
```

## 变更日志

<ChangeLog />
