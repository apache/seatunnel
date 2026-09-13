import ChangeLog from '../changelog/connector-cdc-db2.md';

# DB2 CDC

> DB2 CDC 源连接器

## 支持 DB2 版本

- DB2 LUW 11.5 或 Debezium DB2 连接器支持的更高版本

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

DB2 CDC 连接器可以读取已启用 capture mode 的 DB2 表的快照数据和增量数据。连接器内部使用
Debezium DB2，在初始快照完成后会继续读取已提交的 INSERT、UPDATE 和 DELETE 变更。

## 支持的数据源信息

| 数据源 | 支持版本 | 驱动 | Url | Maven |
|--------|----------|------|-----|-------|
| DB2 | DB2 LUW 11.5 或 Debezium DB2 连接器支持的更高版本 | com.ibm.db2.jcc.DB2Driver | jdbc:db2://127.0.0.1:50000/testdb | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc |

## 使用依赖

### 安装 Jdbc 驱动

#### 对于 Spark/Flink 引擎

> 1. 你需要确保 [DB2 JDBC 驱动 jar 包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已经放置在 `${SEATUNNEL_HOME}/plugins/` 目录中。

#### 对于 SeaTunnel Zeta 引擎

> 1. 你需要确保 [DB2 JDBC 驱动 jar 包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已经放置在 `${SEATUNNEL_HOME}/lib/` 目录中。

## 数据类型映射

| DB2 数据类型 | SeaTunnel 数据类型 |
|--------------|---------------------|
| BOOLEAN | BOOLEAN |
| SMALLINT | SHORT |
| INT<br/>INTEGER | INT |
| BIGINT | BIGINT |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM | DECIMAL |
| REAL | FLOAT |
| DOUBLE<br/>DECFLOAT | DOUBLE |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>DBCLOB<br/>XML | STRING |
| BINARY<br/>VARBINARY<br/>BLOB | BYTES |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |

## 数据源参数

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| username | String | 是 | - | 连接 DB2 时使用的用户名。 |
| password | String | 是 | - | 连接 DB2 时使用的密码。 |
| url | String | 是 | - | DB2 JDBC URL。URL 必须包含数据库名，例如 `jdbc:db2://127.0.0.1:50000/testdb`。 |
| database-names | List | 否 | 从 `url` 中解析出的数据库 | 要监控的数据库名。DB2 CDC 一个 source 监控一个数据库。 |
| table-names | List | 未设置 `table-pattern` 时必填 | - | 要监控的表名，格式为 `databaseName.schemaName.tableName`，例如 `testdb.DB2INST1.CUSTOMERS`。 |
| table-pattern | String | 未设置 `table-names` 时必填 | - | 用于发现已开启 capture mode 的表的正则表达式。 |
| table-names-config | List | 否 | - | 表配置列表。例如：`[{"table": "testdb.DB2INST1.CUSTOMERS","primaryKeys": ["ID"],"snapshotSplitColumn": "ID"}]`。 |
| startup.mode | Enum | 否 | INITIAL | DB2 CDC 的可选启动模式，有效值为 `initial`、`earliest` 和 `latest`。 |
| stop.mode | Enum | 否 | NEVER | DB2 CDC 的可选停止模式，有效值为 `never`。 |
| incremental.parallelism | Integer | 否 | 1 | 增量阶段中并行读取器的数量。 |
| snapshot.split.size | Integer | 否 | 8096 | 表快照的分割大小。 |
| snapshot.fetch.size | Integer | 否 | 1024 | 读取表快照时每次轮询的最大获取大小。 |
| server-time-zone | String | 否 | UTC | 数据库服务器中的会话时区。 |
| connect.timeout.ms | Duration | 否 | 30s | 连接器尝试连接到数据库服务器后，在超时之前等待的最长时间。 |
| connect.max-retries | Integer | 否 | 3 | 连接器重试建立数据库连接的最大次数。 |
| connection.pool.size | Integer | 否 | 20 | 连接池大小。 |
| chunk-key.even-distribution.factor.upper-bound | Double | 否 | 100 | 用于判断分块键是否均匀分布的上界。 |
| chunk-key.even-distribution.factor.lower-bound | Double | 否 | 0.05 | 用于判断分块键是否均匀分布的下界。 |
| sample-sharding.threshold | int | 否 | 1000 | 分块键分布不均时触发采样分片策略的估计分片数阈值。 |
| inverse-sampling.rate | int | 否 | 1000 | 采样分片策略使用的采样率倒数。 |
| exactly_once | Boolean | 否 | false | 启用初始快照切换增量阶段时的精确一次语义。 |
| debezium.* | config | 否 | - | 透传给 Debezium DB2 连接器的配置项。 |
| format | Enum | 否 | DEFAULT | 可选输出格式，有效值为 `DEFAULT` 和 `COMPATIBLE_DEBEZIUM_JSON`。 |
| common-options |  | 否 | - | 源插件通用参数，请参考 [Source Common Options](../common-options/source-common-options.md) 获取详细信息。 |

### 启用 DB2 CDC

DB2 CDC 依赖 DB2 SQL replication 和 ASN capture tables。启用 capture 前，请先确认当前环境具备所需的
IBM replication 授权。运行 SeaTunnel 前，数据库管理员必须先把要读取的表加入 capture mode。可以使用
DB2 控制命令，也可以使用 Debezium 提供的管理 UDF。下面是常见的 UDF 流程：

```sql
VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');
VALUES ASNCDC.ASNCDCSERVICES('start','asncdc');
CALL ASNCDC.ADDTABLE('DB2INST1', 'CUSTOMERS');
VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');
```

完整的 DB2 服务端配置、权限和 ASN capture agent 配置请参考
[Debezium DB2 连接器设置文档](https://debezium.io/documentation/reference/1.9/connectors/db2.html)。

## 任务示例

### 初始读取简单示例

> 该示例先读取初始快照，随后继续读取增量变更。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  DB2-CDC {
    plugin_output = "customers"
    username = "db2inst1"
    password = "db2inst1"
    startup.mode = "initial"
    database-names = ["testdb"]
    table-names = ["testdb.DB2INST1.CUSTOMERS"]
    url = "jdbc:db2://127.0.0.1:50000/testdb"
  }
}

sink {
  console {
    plugin_input = "customers"
  }
}
```

### 增量读取简单示例

> 该示例从最新 DB2 LSN 开始读取，并打印新产生的变更数据。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  DB2-CDC {
    plugin_output = "customers"
    username = "db2inst1"
    password = "db2inst1"
    startup.mode = "latest"
    database-names = ["testdb"]
    table-names = ["testdb.DB2INST1.CUSTOMERS"]
    url = "jdbc:db2://127.0.0.1:50000/testdb"
  }
}

sink {
  console {
    plugin_input = "customers"
  }
}
```

### 支持表的自定义主键

```hocon
source {
  DB2-CDC {
    plugin_output = "customers"
    username = "db2inst1"
    password = "db2inst1"
    startup.mode = "initial"
    database-names = ["testdb"]
    table-names = ["testdb.DB2INST1.CUSTOMERS"]
    table-names-config = [
      {
        table = "testdb.DB2INST1.CUSTOMERS"
        primaryKeys = ["ID"]
        snapshotSplitColumn = "ID"
      }
    ]
    url = "jdbc:db2://127.0.0.1:50000/testdb"
  }
}
```

## 变更日志

<ChangeLog />
