import ChangeLog from '../changelog/connector-cdc-sqlserver.md';

# SQL Server CDC

> Sql Server CDC 源连接器

## 支持 SQL Server 版本

- server:2019（或更高版本，仅供参考）

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要功能

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户定义分割](../../introduction/concepts/connector-v2-features.md)

## 描述

Sql Server CDC 连接器允许从 SqlServer 数据库读取快照数据和增量数据。本文档描述了如何设置 Sql Server CDC 连接器来对 SqlServer 数据库运行 SQL 查询。

:::tip

在通过 JDBC 元数据发现表列信息时，SeaTunnel 会按精确的 schema/table 标识符对返回结果做二次过滤，以避免混入其他表的列（部分驱动会将
`schemaPattern`/`tableNamePattern` 视为 SQL LIKE 模式匹配）。对于大小写敏感的数据库，请确保配置的标识符大小写与数据库一致。

:::

## 支持的数据源信息

| 数据源    | 支持版本                                      | 驱动                                         | Url                                                           | Maven                                                                 |
| --------- | --------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------- |
| SqlServer | <li> server:2019（或更高版本，仅供参考）</li> | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433;databaseName=column_type_test | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc |

## 需要的依赖项

### 安装 Jdbc 驱动

#### 对于 Spark/Flink 引擎

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已经放置在 `${SEATUNNEL_HOME}/plugins/` 目录中。

#### 对于 SeaTunnel Zeta 引擎

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已经放置在 `${SEATUNNEL_HOME}/lib/` 目录中。

## 数据类型映射

|                         SQLserver 数据类型                          | SeaTunnel 数据类型 |
|----------------------------------------------------------------------|---------------------|
| CHAR<br/>VARCHAR<br/>NCHAR<br/>NVARCHAR<br/>TEXT<br/>NTEXT<br/>XML   | STRING              |
| BINARY<br/>VARBINARY<br/>IMAGE                                       | BYTES               |
| INTEGER<br/>INT                                                      | INT                 |
| SMALLINT<br/>TINYINT                                                 | SMALLINT            |
| BIGINT                                                               | BIGINT              |
| FLOAT(1~24)<br/>REAL                                                 | FLOAT               |
| DOUBLE<br/>FLOAT(>24)                                                | DOUBLE              |
| NUMERIC(p,s)<br/>DECIMAL(p,s)<br/>MONEY<br/>SMALLMONEY               | DECIMAL(p, s)       |
| TIMESTAMP                                                            | BYTES               |
| DATE                                                                 | DATE                |
| TIME(s)                                                              | TIME(s)             |
| DATETIME(s)<br/>DATETIME2(s)<br/>DATETIMEOFFSET(s)<br/>SMALLDATETIME | TIMESTAMP(s)        |
| BOOLEAN<br/>BIT<br/>                                                 | BOOLEAN             |

## 数据源参数

| 名称                                           | 类型     | 是否必填 | 默认值  | 描述                                                                                                                                                                                                                                                                                                                |
| ---------------------------------------------- | -------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| username                                       | String   | 是       | -       | 连接数据库服务器时使用的数据库名称。                                                                                                                                                                                                                                                                                |
| password                                       | String   | 是       | -       | 连接数据库服务器时使用的密码。                                                                                                                                                                                                                                                                                      |
| database-names                                 | List     | 是       | -       | 要监控的数据库名称。                                                                                                                                                                                                                                                                                                |
| table-names                                    | List     | 是       | -       | 表名是模式名和表名的组合 (databaseName.schemaName.tableName)。                                                                                                                                                                                                                                                      |
| table-names-config                             | List     | 否       | -       | 表配置列表。例如：[{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]                                                                                                                                                                                                           |
| url                                            | String   | 是       | -       | URL 必须包含数据库，如 "jdbc:sqlserver://localhost:1433;databaseName=test"。                                                                                                                                                                                                                                        |
| startup.mode                                   | Enum     | 否       | INITIAL | SqlServer CDC 消费者的可选启动模式，有效枚举为 "initial"、"earliest"、"latest" 和 "specific"。                                                                                                                                                                                                                      |
| startup.timestamp                              | Long     | 否       | -       | 从指定的纪元时间戳（以毫秒为单位）开始。<br/> **注意，当 "startup.mode" 选项使用 `'timestamp'` 时，此选项是必需的。**                                                                                                                                                                                               |
| startup.specific-offset.file                   | String   | 否       | -       | 从指定的 binlog 文件名开始。<br/>**注意，当 "startup.mode" 选项使用 `'specific'` 时，此选项是必需的。**                                                                                                                                                                                                             |
| startup.specific-offset.pos                    | Long     | 否       | -       | 从指定的 binlog 文件位置开始。<br/>**注意，当 "startup.mode" 选项使用 `'specific'` 时，此选项是必需的。**                                                                                                                                                                                                           |
| stop.mode                                      | Enum     | 否       | NEVER   | SqlServer CDC 消费者的可选停止模式，有效枚举为 "never"。                                                                                                                                                                                                                                                            |
| stop.timestamp                                 | Long     | 否       | -       | 在指定的纪元时间戳（以毫秒为单位）停止。<br/>**注意，当 "stop.mode" 选项使用 `'timestamp'` 时，此选项是必需的。**                                                                                                                                                                                                   |
| stop.specific-offset.file                      | String   | 否       | -       | 在指定的 binlog 文件名停止。<br/>**注意，当 "stop.mode" 选项使用 `'specific'` 时，此选项是必需的。**                                                                                                                                                                                                                |
| stop.specific-offset.pos                       | Long     | 否       | -       | 在指定的 binlog 文件位置停止。<br/>**注意，当 "stop.mode" 选项使用 `'specific'` 时，此选项是必需的。**                                                                                                                                                                                                              |
| incremental.parallelism                        | Integer  | 否       | 1       | 增量阶段中并行读取器的数量。                                                                                                                                                                                                                                                                                        |
| snapshot.split.size                            | Integer  | 否       | 8096    | 表快照的分割大小（行数），读取表快照时，捕获的表会被分割为多个分割。                                                                                                                                                                                                                                                |
| snapshot.fetch.size                            | Integer  | 否       | 1024    | 读取表快照时每次轮询的最大获取大小。                                                                                                                                                                                                                                                                                |
| server-time-zone                               | String   | 否       | UTC     | 数据库服务器中的会话时区。                                                                                                                                                                                                                                                                                          |
| connect.timeout                                | Duration | 否       | 30s     | 连接器尝试连接到数据库服务器后在超时之前应该等待的最长时间。                                                                                                                                                                                                                                                        |
| connect.max-retries                            | Integer  | 否       | 3       | 连接器应该重试建立数据库服务器连接的最大重试次数。                                                                                                                                                                                                                                                                  |
| connection.pool.size                           | Integer  | 否       | 20      | 连接池大小。                                                                                                                                                                                                                                                                                                        |
| chunk-key.even-distribution.factor.upper-bound | Double   | 否       | 100     | 分块键分布因子的上界。此因子用于确定表数据是否均匀分布。如果计算的分布因子小于或等于此上界（即，(MAX(id) - MIN(id) + 1) / 行数），表分块将被优化以实现均匀分布。否则，如果分布因子较大，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，表将被视为不均匀分布并使用基于采样的分片策略。默认值为 100.0。   |
| chunk-key.even-distribution.factor.lower-bound | Double   | 否       | 0.05    | 分块键分布因子的下界。此因子用于确定表数据是否均匀分布。如果计算的分布因子大于或等于此下界（即，(MAX(id) - MIN(id) + 1) / 行数），表分块将被优化以实现均匀分布。否则，如果分布因子较小，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，表将被视为不均匀分布并使用基于采样的分片策略。默认值为 0.05。    |
| sample-sharding.threshold                      | int      | 否       | 1000    | 此配置指定了触发采样分片策略的估计分片数阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且估计的分片数（计算为近似行数 / 分块大小）超过此阈值时，将使用采样分片策略。这可以帮助更有效地处理大型数据集。默认值为 1000 分片。 |
| inverse-sampling.rate                          | int      | 否       | 1000    | 采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。对于非常大的数据集，首选较低的采样率时，此选项特别有用。默认值为 1000。                                                                            |
| exactly_once                                   | Boolean  | 否       | false   | 启用精确一次语义。                                                                                                                                                                                                                                                                                                  |
| debezium.*                                     | config   | 否       | -       | 将 Debezium 的属性传递给 Debezium Embedded Engine，用于捕获来自 SqlServer 服务器的数据变更。<br/>了解更多关于<br/>[Debezium 的 SqlServer 连接器属性](https://github.com/debezium/debezium/blob/1.6/documentation/modules/ROOT/pages/connectors/sqlserver.adoc#connector-properties)                                 |
| format                                         | Enum     | 否       | DEFAULT | SqlServer CDC 的可选输出格式，有效枚举为 "DEFAULT"、"COMPATIBLE_DEBEZIUM_JSON"。                                                                                                                                                                                                                                    |
| common-options                                 |          | 否       | -       | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 获取详细信息。                                                                                                                                                                                                                                     |

### 启用 Sql Server CDC

1. 检查 CDC 代理是否启用

> `EXEC xp_servicecontrol N'querystate', N'SQLServerAGENT';` <br/>
> 如果结果是运行中，证明它已经启用。否则，您需要手动启用它

2. 启用 CDC 代理

> /opt/mssql/bin/mssql-conf setup

3. 结果如下

> 1) 评估版（免费，无生产使用权，180天限制）
> 2) 开发者版（免费，无生产使用权）
> 3) 快速版（免费）
> 4) Web 版（付费）
> 5) 标准版（付费）
> 6) 企业版（付费）
> 7) 企业核心版（付费）
> 8) 我通过零售销售渠道购买了许可证，并有产品密钥要输入。

4. 在数据库级别设置 CDC
   在下面的数据库级别设置以启用 CDC。在此级别，启用 CDC 的数据库下的所有表都会自动启用 CDC

> USE TestDB; -- 替换为实际的数据库名称 <br/>
> EXEC sys.sp_cdc_enable_db;<br/>
> SELECT name, is_tracked_by_cdc  FROM sys.tables  WHERE name = 'table'; -- table 替换为您要检查的表名

## 任务示例

### 初始读取简单示例

> 这是一个流模式 CDC，初始化读取表数据，成功读取后将进行增量读取。以下 SQL DDL 仅供参考

```
env {
  # 您可以在这里设置引擎配置
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  SqlServer-CDC {
    plugin_output = "customers"
    username = "sa"
    password = "Y.sa123456"
    startup.mode="initial"
    database-names = ["column_type_test"]
    table-names = ["column_type_test.dbo.full_types"]
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
  }
}

transform {
}

sink {
  console {
    plugin_input = "customers"
  }
}
```

### 增量读取简单示例

> 这是一个增量读取，读取变更的数据进行打印

```
env {
  # 您可以在这里设置引擎配置
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  SqlServer-CDC {
   # 设置精确一次读取
    exactly_once=true 
    plugin_output = "customers"
    username = "sa"
    password = "Y.sa123456"
    startup.mode="latest"
    database-names = ["column_type_test"]
    table-names = ["column_type_test.dbo.full_types"]
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
  }
}

transform {
}

sink {
  console {
    plugin_input = "customers"
  }
}
```

### 支持表的自定义主键

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  SqlServer-CDC {
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "sa"
    password = "Y.sa123456"
    database-names = ["column_type_test"]
    
    table-names = ["column_type_test.dbo.simple_types", "column_type_test.dbo.full_types"]
    table-names-config = [
      {
        table = "column_type_test.dbo.full_types"
        primaryKeys = ["id"]
      }
    ]
  }
}

sink {
  console {
  }
}
```

## 变更日志

<ChangeLog />
