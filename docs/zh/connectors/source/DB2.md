import ChangeLog from '../changelog/connector-jdbc.md';

# DB2

> JDBC DB2 Source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 读取 DB2 数据。DB2 需要使用 IBM 的 `db2jcc` 驱动，出于许可证原因 SeaTunnel 不内置该驱动。在 source 块中使用 `Jdbc` 插件名称，并将 `driver` 设置为 `com.ibm.db2.jcc.DB2Driver`。

## 使用依赖关系

### 适用于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 适用于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL，可以实现列投影。

## 支持的数据源信息

| 数据源 | 支持版本                                  | 驱动                       | Url                            | Maven                                                          |
|--------|-------------------------------------------|----------------------------|--------------------------------|----------------------------------------------------------------|
| DB2    | 不同的依赖版本有不同的驱动程序类。       | com.ibm.db2.jcc.DB2Driver  | jdbc:db2://127.0.0.1:50000/dbname | [下载](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) |

## 数据库依赖

> 请下载 "Maven" 对应的支持列表，并将其复制到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 工作目录下<br/>
> 例如 DB2 数据源：cp db2-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|                                            DB2 数据类型                                            | SeaTunnel 数据类型 |
|------------------------------------------------------------------------------------------------------|--------------------|
| BOOLEAN                                                                                              | BOOLEAN            |
| SMALLINT                                                                                             | SHORT              |
| INT<br/>INTEGER                                                                                      | INTEGER            |
| BIGINT                                                                                               | LONG               |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM                                                                  | DECIMAL(38,18)     |
| REAL                                                                                                 | FLOAT              |
| FLOAT<br/>DOUBLE<br/>DOUBLE PRECISION<br/>DECFLOAT                                                   | DOUBLE             |
| CHAR<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>LONG VARGRAPHIC<br/>DBCLOB | STRING             |
| BLOB                                                                                                 | BYTES              |
| DATE                                                                                                 | DATE               |
| TIME                                                                                                 | TIME               |
| TIMESTAMP                                                                                            | TIMESTAMP          |
| ROWID<br/>XML                                                                                        | 暂不支持           |

## 源选项

|             名称            |    类型    | 是否必填 | 默认值 | 描述                                                                                                                                                                    |
|------------------------------|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | 是      | -       | JDBC 连接 URL，例如 `jdbc:db2://127.0.0.1:50000/dbname`。                                                                                                                  |
| driver                       | String     | 是      | -       | JDBC 驱动类名，DB2 使用 `com.ibm.db2.jcc.DB2Driver`。                                                                                                                    |
| username                     | String     | 否       | -       | DB2 用户名。`user` 也可作为 `username` 的备选键。                                                                                                                          |
| password                     | String     | 否       | -       | DB2 密码。                                                                                                                                                                 |
| query                        | String     | 是      | -       | 读取数据的 SELECT 语句。SELECT 的列列表决定输出 schema，只选择需要的列即可。                                                                                              |
| connection_check_timeout_sec | Int        | 否       | 30      | 连接校验超时时间（秒），超过该时间未完成则失败。                                                                                                                          |
| partition_column             | String     | 否       | -       | 用于并行拆分读取的列。支持数值列和字符串列（配合 `split.string_split_mode` 使用）；只能配置一列。                                                                          |
| partition_lower_bound        | String     | 否       | -       | `partition_column` 的下界，用于范围拆分；不设置时 SeaTunnel 查询最小值。                                                                                                  |
| partition_upper_bound        | String     | 否       | -       | `partition_column` 的上界，用于范围拆分；不设置时 SeaTunnel 查询最大值。                                                                                                  |
| partition_num                | Int        | 否       | 10      | 并行读取时的拆分数量，默认值为 `10`。如果 `env.parallelism` 更大并希望每个读取任务一个拆分，可适当上调。                                                                    |
| fetch_size                   | Int        | 否       | 0       | JDBC 读取时的 fetch size。`0` 表示使用驱动默认值；对于返回大量行的查询，可设为正值以减少数据库往返次数。                                                                  |
| properties                   | Map        | 否       | -       | 额外的 JDBC 连接参数。当 `properties` 与 `url` 包含相同键时，优先级由驱动决定。                                                                                            |
| common-options               |            | 否       | -       | Source 插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                                                                                       |

### 小贴士

> 不配置 `partition_column` 时，源端按单拆分读取；配置后，SeaTunnel 会把表固定切分为 `partition_num`（默认 10）个 split，与 `env.parallelism` 无关；同时并发读取的 split 数上限为 `min(partition_num, env.parallelism)`——当 `parallelism > partition_num` 时多余的 reader 槽位会闲置，当 `parallelism < partition_num` 时部分 reader 会顺序消费多个 split。两者互相独立，并不是"取较大值"。

## 任务示例

### 简单示例

此示例在 DB2 中以两个并行读取器查询 `type_bin` 的所有字段并打印到控制台。

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    connection_check_timeout_sec = 100
    username = "db2inst1"
    password = "123456"
    query = "select * from type_bin"
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
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
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
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    query = "select * from type_bin"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 500
    partition_num = 10
  }
}
```

## 变更日志

<ChangeLog />
