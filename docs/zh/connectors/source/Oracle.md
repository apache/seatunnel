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
| CHAR<br/>NCHAR<br/>VARCHAR<br/>NVARCHAR2<br/>VARCHAR2<br/>LONG<br/>ROWID<br/>NCLOB<br/>CLOB<br/>XML | STRING |
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

## 变更日志

<ChangeLog />
