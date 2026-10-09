import ChangeLog from '../changelog/connector-jdbc.md';

# DuckDB

> JDBC DuckDB Sink 连接器

## 支持 DuckDB 版本

- 0.8.x/0.9.x/0.10.x/1.x

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 将数据写入 DuckDB 数据库文件。支持批处理和流处理两种模式，也支持并发写入。此连接器使用的 DuckDB JDBC 驱动没有提供 XA 数据源，因此 DuckDB 无法使用 JDBC Sink 基于 XA 的精确一次选项。DuckDB 是进程内数据库，因此连接器对接的是本地数据库文件路径（`jdbc:duckdb:/path/to/database.db`）或内存数据库。生成的 DECIMAL DDL 与默认 locale 无关。

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要功能

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

> 通用 JDBC Sink 通过 XA 事务实现精确一次；DuckDB JDBC 驱动没有 XA 数据源。DuckDB 作业不要设置 `is_exactly_once = true`。

## 支持的数据源信息

| 数据源    | 支持的版本              | 驱动器                     | 网址                               | Maven下载链接                                                       |
|--------|--------------------|-------------------------|----------------------------------|-----------------------------------------------------------------|
| DuckDB | 不同的依赖版本具有不同的驱动程序类。 | org.duckdb.DuckDBDriver | jdbc:duckdb:/path/to/database.db | [下载](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) |

## 数据类型映射

| SeaTunnel 数据类型                  | DuckDB 数据类型    |
|---------------------------------|----------------|
| BOOLEAN                         | BOOLEAN        |
| TINYINT<br/>SMALLINT<br/>INT    | INTEGER        |
| BIGINT                          | BIGINT         |
| DECIMAL(x,y)(获取指定列的指定列大小.<38)   | DECIMAL(x,y)   |
| DECIMAL(x,y)(获取指定列的指定列大小.>38)   | DECIMAL(38,18) |
| FLOAT                           | FLOAT          |
| DOUBLE                          | DOUBLE         |
| STRING                          | VARCHAR        |
| DATE                            | DATE           |
| TIME                            | TIME           |
| TIMESTAMP                       | TIMESTAMP      |
| BYTES<br/>ARRAY<br/>ROW<br/>MAP | BLOB           |

JDBC 连接器读取和写入 DuckDB `TIME` 时保留微秒精度。该类型表示不带时区的本地时刻。

## Sink 选项

| 名称                           | 类型      | 是否必需 | 默认值                          | 描述                                                                                          |
|------------------------------|---------|------|------------------------------|---------------------------------------------------------------------------------------------|
| url                          | String  | 是    | -                            | JDBC 连接的 URL。参考案例：jdbc:duckdb:/path/to/database.db                                          |
| driver                       | String  | 是    | -                            | 用于连接到远程数据源的 jdbc 类名，<br/> 如果您使用 DuckDB，值为 `org.duckdb.DuckDBDriver`。                        |
| username                     | String  | 否    | -                            | 连接实例用户名                                                                                     |
| password                     | String  | 否    | -                            | 连接实例密码                                                                                      |
| query                        | String  | 否    | -                            | 使用此 sql 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 具有更高的优先级                                       |
| database                     | String  | 否    | -                            | 使用此 `database` 和 `table-name` 自动生成 sql 并接收上游输入数据写入数据库。<br/>仅当 `generate_sink_sql = true` 时用于自动生成 SQL；设置 `query` 时以 `query` 为准。        |
| table                        | String  | 否    | -                            | 使用数据库和此表名自动生成 sql 并接收上游输入数据写入数据库。<br/>仅当 `generate_sink_sql = true` 时用于自动生成 SQL；设置 `query` 时以 `query` 为准。                             |
| primary_keys                 | Array   | 否    | -                            | 此选项用于在自动生成 sql 时支持 `insert`、`delete` 和 `update` 等操作。                                        |
| connection_check_timeout_sec | Int     | 否    | 30                           | 等待用于验证连接的数据库操作完成的时间（以秒为单位）。                                                                 |
| max_retries                  | Int     | 否    | 0                            | 提交失败（executeBatch）的重试次数                                                                     |
| batch_size                   | Int     | 否    | 1000                         | 对于批量写入，当缓冲记录数达到 `batch_size` 数量或时间达到 `checkpoint.interval`<br/>时，数据将被刷新到数据库中                |
| ducklake_bulk_write          | Boolean | 否    | false                        | 对已有 DuckLake 表，先将每批数据暂存到 DuckDB 临时表，再用一条 `INSERT ... SELECT` 写入湖表；见下文。                 |
| ducklake_bulk_write_ignore_inherited_keys | Boolean | 否 | false | 明确忽略源端 PK/UNIQUE 元数据，仅用于 bulk INSERT 追加；要求 ducklake_bulk_write=true，仍不支持显式 primary_keys。 |
| is_exactly_once              | Boolean | 否    | false                        | 通用 JDBC 的 XA 选项。DuckDB JDBC 驱动没有 XA 数据源，应保持 `false`。                                          |
| generate_sink_sql            | Boolean | 否    | false                        | 根据您要写入的数据库表生成 sql 语句                                                                        |
| xa_data_source_class_name    | String  | 否    | -                            | 通用 JDBC 的 XA 数据源类名选项。DuckDB JDBC 驱动没有提供该类，不能借此为 DuckDB 启用精确一次。                        |
| max_commit_attempts          | Int     | 否    | 3                            | 事务提交失败的重试次数                                                                                 |
| transaction_timeout_sec      | Int     | 否    | -1                           | 事务打开后的超时时间，默认为 -1（永不超时）。请注意，设置超时可能会影响<br/>精确一次语义                                            |
| auto_commit                  | Boolean | 否    | true                         | 默认启用自动事务提交                                                                                  |
| field_ide                    | String  | 否    | -                            | 标识从源同步到接收器时字段是否需要转换。`ORIGINAL` 表示不需要转换；`UPPERCASE` 表示转换为大写；`LOWERCASE` 表示转换为小写。             |
| properties                   | Map     | 否    | -                            | 附加连接配置参数，当 properties 和 URL 具有相同参数时，优先级由 <br/>驱动程序的具体实现确定。例如，在 DuckDB 中，properties 优先于 URL。 |
| common-options               |         | 否    | -                            | Sink 插件通用参数，详情请参考 [Sink Common Options](../common-options/sink-common-options.md)                          |
| schema_save_mode             | Enum    | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST | 在同步任务开启之前，针对目标端已有的表结构选择不同的处理方案。                                                             |
| data_save_mode               | Enum    | 否    | APPEND_DATA                  | 在同步任务开启之前，针对目标端已有数据选择不同的处理方案。                                                               |
| custom_sql                   | String  | 否    | -                            | 当 data_save_mode 选择 CUSTOM_PROCESSING 时，应填写 CUSTOM_SQL 参数。此参数通常填写可执行的 SQL。SQL 将在同步任务之前执行。   |
| enable_upsert                | Boolean | 否    | true                         | 通过 primary_keys 存在启用 upsert，如果任务只有 `insert`，将此参数设置为 `false` 可以加快数据导入速度                      |
| multi_table_sink_replica     | Int     | 否    | 1                            | 多表写入时的写入器副本数。当 `multi_table_sink_replica > 1` 时，多表并行写入。                                                |

### 提示

> 如果未设置 partition_column，它将以单一并发运行，如果设置了 partition_column，它将根据任务的并发度并行执行。

## 任务示例

### DuckLake 批量追加

已有的 JDBC Sink 可以写入 DuckLake，但 DuckDB JDBC 1.3.1 对湖表执行 `executeBatch` 时可能每行生成一个 Parquet 文件。开启 `ducklake_bulk_write` 后，每批最多 `batch_size` 行先写入连接内的 DuckDB 临时表，再用一条 SQL 写入湖表。目标表必须预先存在。

通过 `session_init_sql_file` 初始化**每个 Worker 的每条连接**，重连时也会重新执行。例如 `/etc/seatunnel/ducklake-init.sql`：

```sql
LOAD ducklake;
LOAD postgres_scanner;
LOAD httpfs;
-- 在各 Worker 安全配置 PostgreSQL 与 S3 凭据，不要写入作业配置。
ATTACH 'ducklake:postgres:dbname=lake_metadata host=metadata.example.com port=5432'
  AS lake (METADATA_SCHEMA 'lake_catalog', DATA_PATH 's3://my-bucket/ducklake/');
```

`lake_metadata` 是 PostgreSQL **数据库名**，`lake_catalog` 是其元数据 **schema**；DuckLake 表的 `main` schema 是另一层命名。实际部署时按真实实例、schema 和数据地址配置，并确保每个 Worker 都能访问初始化脚本和对应版本的扩展。

```hocon
sink {
  Jdbc {
    url = "jdbc:duckdb:;session_init_sql_file=/etc/seatunnel/ducklake-init.sql"
    driver = "org.duckdb.DuckDBDriver"
    database = "lake"
    table = "main.events"
    generate_sink_sql = true
    ducklake_bulk_write = true
    schema_save_mode = "IGNORE"
    data_save_mode = "APPEND_DATA"
    batch_size = 1000
    auto_commit = true
    max_retries = 0
  }
}
```

该模式仅接受 INSERT 行，不支持 `query`、主键更新、COPY、XA、自动建表或 JDBC 批次自动重试。每次成功 flush 会提交一次湖表写入；如果提交结果不明而作业重放，仍可能出现重复行。Parquet 文件数还受 DuckLake 分区和文件大小策略影响，不能保证任何场景都严格每批一个文件。关闭此选项时，普通 DuckDB 写入保持原样。

该模式只写入一个配置的目标表，不支持多表路由。默认仍拒绝从上游表继承的主键或 UNIQUE 键。对于 PostgreSQL 等只产生 INSERT 的批量源，可以明确设置 `ducklake_bulk_write_ignore_inherited_keys = true`，只在 Sink schema 副本中忽略继承键，保留源端元数据。目标表必须预建且不包含强制键约束；显式配置 `primary_keys` 仍不支持。此选项不启用 upsert、不保证唯一性、不对重放去重，UPDATE 和 DELETE 行仍会报错。仅关闭 `enable_upsert` 不会移除继承键。

按行宽和 Worker 内存预算选择 `batch_size`。小行可以使用比默认 1000 更大的批次减少小文件，但 flush 期间数据同时占用 Java 缓冲和 DuckDB 暂存表。检查点、批次时间间隔和作业结束也会触发未满批次的 flush，因此只增大 `batch_size` 不能保证大文件。每次 flush 重建临时表以释放上一批的存储；关闭写入器时删除剩余临时表，同时保留池中的物理连接。这会增加每批建表和准备语句的开销。尚未测量大批次的吞吐量和峰值内存，小规模回归用例不能证明生产容量。

并行写入器会独立提交到同一目标表；checkpoint 不会将这些提交合并为一个湖表事务。每次成功 flush 都是一次独立提交，频繁的小批次会增加元数据操作和快照创建。Sink 不会重试提交失败（`max_retries = 0`），包括 DuckLake 向调用方返回的提交冲突。请根据元数据后端选择写入并行度，并在增加并行度前验证并发写入。

### 简单

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    row_num = 1000
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        email = "string"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = "duckdb"
    password = ""
  }
}
```

### CDC（变更数据捕获）事件

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://localhost:3306/test"
    username = "root"
    password = "123456"
    table-names = ["test.user"]
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = "duckdb"
    password = ""
    generate_sink_sql = true
    # 您需要同时配置 database 和 table
    database = main
    table = "sink_table"
    primary_keys = ["id"]
  }
}
```

## Changelog

<ChangeLog />
