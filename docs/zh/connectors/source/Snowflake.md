import ChangeLog from '../changelog/connector-jdbc.md';

# Snowflake

> JDBC Snowflake 源连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 读取 Snowflake 数据。SeaTunnel 使用官方 Snowflake JDBC 驱动以及 JDBC Source 插件。请在 `url` 中填入 Snowflake 账户标识，并通过 `query` 控制输出 schema（只选择需要的列）。

## 数据库依赖

> 请下载 "Maven" 对应的支持列表，并将其复制到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 工作目录下<br/>
> 例如 Snowflake 数据源：cp snowflake-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL，可以实现列投影。

## 支持的数据源信息

| 数据源 | 支持版本                                  | 驱动                                       | Url                                          | Maven                                                          |
|--------|-------------------------------------------|--------------------------------------------|----------------------------------------------|----------------------------------------------------------------|
| Snowflake | 不同的依赖版本有不同的驱动程序类。    | net.snowflake.client.jdbc.SnowflakeDriver | jdbc:snowflake://<account_name>.snowflakecomputing.com | [下载](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) |

## 数据类型映射

|                             Snowflake 数据类型                            | SeaTunnel 数据类型 |
|-----------------------------------------------------------------------------|--------------------|
| BOOLEAN                                                                     | BOOLEAN            |
| TINYINT<br/>SMALLINT<br/>BYTEINT                                            | SHORT              |
| INT<br/>INTEGER                                                             | INT                |
| BIGINT                                                                      | LONG               |
| DECIMAL<br/>NUMERIC<br/>NUMBER<br/>                                         | DECIMAL(p, s)      |
| DECIMAL(p, s)（p > 38 时）                                                  | DECIMAL(38, 18)    |
| REAL<br/>FLOAT4                                                             | FLOAT              |
| DOUBLE<br/>DOUBLE PRECISION<br/>FLOAT8<br/>FLOAT                            | DOUBLE             |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>STRING<br/>TEXT<br/>VARIANT<br/>OBJECT   | STRING             |
| DATE                                                                        | DATE               |
| TIME                                                                        | TIME               |
| DATETIME<br/>TIMESTAMP<br/>TIMESTAMP_LTZ<br/>TIMESTAMP_NTZ<br/>TIMESTAMP_TZ | TIMESTAMP          |
| BINARY<br/>VARBINARY                                                        | BYTES              |
| GEOGRAPHY (WKB 或 EWKB)<br/>GEOMETRY (WKB 或 EWKB)                          | BYTES              |
| GEOGRAPHY (GeoJSON, WKT 或 EWKT)<br/>GEOMETRY (GeoJSON, WKB 或 EWKB)        | STRING             |

## 源选项

|             名称            |    类型    | 是否必填 | 默认值 | 描述                                                                                                                                                                                |
|------------------------------|------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | 是      | -       | JDBC 连接 URL，例如 `jdbc:snowflake://<account_name>.snowflakecomputing.com`。可以在 URL 后追加 Snowflake JDBC 参数（如 `?GEOGRAPHY_OUTPUT_FORMAT='EWKT'`）。                          |
| driver                       | String     | 是      | -       | JDBC 驱动类名，Snowflake 使用 `net.snowflake.client.jdbc.SnowflakeDriver`。                                                                                                          |
| username                     | String     | 否       | -       | Snowflake 账户用户名。                                                                                                                                                              |
| password                     | String     | 否       | -       | Snowflake 账户密码。                                                                                                                                                                |
| query                        | String     | 是      | -       | 读取数据的 SELECT 语句。SELECT 的列列表决定输出 schema，只选择需要的列即可。                                                                                                          |
| connection_check_timeout_sec | Int        | 否       | 30      | 连接校验超时时间（秒），超过该时间未完成则失败。                                                                                                                                    |
| partition_column             | String     | 否       | -       | 用于并行拆分读取的列。支持数值列和字符串列（配合 `split.string_split_mode` 使用）；只能配置一列。                                                                                    |
| partition_lower_bound        | String     | 否       | -       | `partition_column` 的下界，用于范围拆分；不设置时 SeaTunnel 查询最小值。                                                                                                            |
| partition_upper_bound        | String     | 否       | -       | `partition_column` 的上界，用于范围拆分；不设置时 SeaTunnel 查询最大值。                                                                                                            |
| partition_num                | Int        | 否       | 10      | 并行读取时的拆分数量，默认值为 `10`。如果 `env.parallelism` 更大并希望每个读取任务一个拆分，可适当上调。                                                                              |
| fetch_size                   | Int        | 否       | 0       | JDBC 读取时的 fetch size。`0` 表示使用驱动默认值；返回大量行时可设为正值以减少数据库往返。                                                                                          |
| properties                   | Map        | 否       | -       | 额外的 JDBC 连接参数。`properties` 与 `url` 包含相同键时，优先级由驱动决定。                                                                                                        |
| common-options               |            | 否       | -       | Source 插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                                                                                                |

### 小贴士

> 不配置 `partition_column` 时，源端按单拆分读取；配置后，SeaTunnel 会按 `partition_num`（默认 10）和作业并行度两者中较大的值并行读取。
>
> Snowflake JDBC URL 参数（如 `GEOGRAPHY_OUTPUT_FORMAT`）可通过 `?` 直接追加，示例：`?GEOGRAPHY_OUTPUT_FORMAT='EWKT'`。完整可配置参数及地理空间类型请参考 Snowflake [Geospatial Data Types](https://docs.snowflake.com/en/sql-reference/data-types-geospatial)。

## 任务示例

### 简单示例

此示例从 Snowflake 中查询 `type_bin` 的所有字段并打印到控制台。

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    connection_check_timeout_sec = 100
    username = "USER"
    password = "PASSWORD"
    query = "select * from type_bin limit 16"
  }
}

sink {
  Console {}
}
```

### 按数值列并行读取

按数值的 `partition_column` 并行读取整张表，让 SeaTunnel 自动查询上下界。

```hocon
source {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    query = "select * from type_bin"
    partition_column = "id"
    partition_num = 10
  }
}
```

### 显式指定上下界的并行读取

显式给出 `partition_lower_bound` 与 `partition_upper_bound`，可跳过 SeaTunnel 为学习列范围额外发出的 `MIN`/`MAX` 查询。

```hocon
source {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    query = "select * from type_bin"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 500
    partition_num = 10
  }
}
```

## 说明

- Snowflake 任务使用 `Jdbc` 插件名，并设置 `driver = "net.snowflake.client.jdbc.SnowflakeDriver"`。
- 运行任务前请把 Snowflake JDBC 驱动 jar 放到 `$SEATUNNEL_HOME/plugins/jdbc/lib/`。
- 并行读取时，`partition_column`、`partition_lower_bound`、`partition_upper_bound` 和 `partition_num` 必须描述同一数值列的范围。
- Snowflake 地理空间列的字节/字符串返回形式由 Snowflake JDBC URL 参数（如 `GEOGRAPHY_OUTPUT_FORMAT`）决定。

## 变更日志

<ChangeLog />
