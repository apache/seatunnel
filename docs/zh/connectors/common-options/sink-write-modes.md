---
sidebar_position: 3
---

# Sink 写入模式与 Save Mode

Sink 配置里有两个容易混淆的决策：

- **写入模式**决定 SeaTunnel 如何把每一行数据写到目标端。
- **Save Mode**决定 SeaTunnel 在开始写入数据前，如何处理目标端已经存在的表、索引、目录或数据。

当你需要在 `generate_sink_sql`、`query`、`schema_save_mode`、`data_save_mode`、`custom_sql`、`primary_keys`、`enable_upsert` 之间做选择时，可以先看这一页。

## 快速决策表

| 目标 | 优先选择 | 说明 |
|------|----------|------|
| 让 SeaTunnel 为 JDBC 目标端生成 INSERT / UPSERT / UPDATE / DELETE SQL | `generate_sink_sql = true`，并配置 `database`、`table`，通常还要配置 `primary_keys` | 这是 JDBC Sink 的能力。SeaTunnel 能解析目标 Catalog 表时，也可以执行 save mode 和自动建表。 |
| 完全控制 JDBC 写入 SQL | `query = "INSERT ... VALUES (?, ...)"` | 不要和 `generate_sink_sql = true` 同时配置。JDBC Sink 在这个模式下不会执行 `schema_save_mode`、`data_save_mode` 或 `custom_sql`。 |
| 目标表不存在时自动创建，或不存在时报错 | `schema_save_mode` | 仅适用于显式暴露 save mode 参数，并且能通过 Catalog 创建或检查目标端的 Sink。 |
| 写入前保留、清空或检查目标端已有数据 | `data_save_mode` | 支持值取决于具体 connector。File Sink 通常只支持 `DROP_DATA`、`APPEND_DATA`、`ERROR_WHEN_DATA_EXISTS`。 |
| 写入数据前先执行一条自定义 SQL | `data_save_mode = "CUSTOM_PROCESSING"` 和 `custom_sql` | 仅适用于同时暴露这两个参数的 connector。这是写入前钩子，不是逐行写入 SQL。 |
| JDBC Sink 使用数据库原生 Upsert | `generate_sink_sql = true`、`primary_keys`、`enable_upsert = true` | 没有可用主键或唯一键时，JDBC 自动生成 SQL 会退化为普通 INSERT。 |
| 写入对象存储或文件系统 | 查看具体 File Sink 参数表 | File Sink 不使用 `generate_sink_sql`。部分文件 connector 暴露 save mode，部分不暴露。 |

## JDBC：`generate_sink_sql` 与 `query`

JDBC Sink 有两种互斥的写入模式。

| 模式 | 必需参数 | 是否执行 Save Mode | 典型场景 |
|------|----------|-------------------|----------|
| 自动生成 SQL | `generate_sink_sql = true`、`database`，通常还有 `table` | 是，前提是能解析目标 Catalog 表 | 大多数数据库写入、CDC 写入、自动建表、upsert、update、delete |
| 自定义 SQL | `query = "INSERT ... VALUES (?, ...)"` | 否 | 必须完全控制目标 SQL，并接受跳过 save mode 处理 |

不要同时配置 `generate_sink_sql = true` 和 `query`。

使用 `generate_sink_sql = true` 时，如果目标端需要处理 UPDATE、DELETE 或 upsert 记录，请配置 `primary_keys`。如果没有显式配置 `primary_keys`，SeaTunnel 会尝试从上游 Catalog 元数据继承主键，再尝试第一组唯一键；仍然没有可用键时，会退化为普通 INSERT。

## Save Mode 语义

### `schema_save_mode`

`schema_save_mode` 控制写入前如何处理目标结构。

| 值 | 行为 |
|----|------|
| `RECREATE_SCHEMA` | 目标不存在时创建；目标已存在时删除后重建。 |
| `CREATE_SCHEMA_WHEN_NOT_EXIST` | 仅在目标不存在时创建。 |
| `ERROR_WHEN_SCHEMA_NOT_EXIST` | 目标不存在时报错。 |
| `IGNORE` | 跳过结构处理。 |

对于数据库 Sink，目标通常是表；对于文件 Sink，目标通常是路径或目录。

### `data_save_mode`

`data_save_mode` 控制写入前如何处理目标端已有数据。

| 值 | 行为 |
|----|------|
| `DROP_DATA` | 保留结构并清空已有数据。 |
| `APPEND_DATA` | 保留已有数据并追加写入。 |
| `CUSTOM_PROCESSING` | 写入前执行 `custom_sql`。仅适用于同时暴露这两个参数的 connector。 |
| `ERROR_WHEN_DATA_EXISTS` | 发现已有数据时报错。 |

这些参数不是所有 connector 都支持。最终请以你正在使用版本的具体 connector 参数表为准。

## Connector 支持边界

### JDBC 系列 Sink

JDBC Sink 以及 MySQL、PostgreSQL、Oracle、SQL Server 等 JDBC 系列 Sink 页面使用同一套 JDBC 写入模式：

- 支持 `generate_sink_sql`。
- 支持 `query`。
- 自动生成 SQL 模式下支持 `schema_save_mode` 和 `data_save_mode`。
- `custom_sql` 只有在 save mode 处理真正执行时才会执行。
- `enable_upsert` 只有在 SeaTunnel 拿到可用主键或唯一键后才有意义。

完整参数和示例请看 [JDBC Sink](../sink/Jdbc.md)。

### Doris Sink

Doris Sink 支持 `schema_save_mode`、`data_save_mode`、`custom_sql` 和 `save_mode_create_template`，但不使用 JDBC 的 `generate_sink_sql`。

如果要处理 CDC DELETE 事件，还需要 Doris 侧支持删除能力，并按场景配置 connector 的 `sink.enable-delete`。详见 [Doris Sink](../sink/Doris.md)。

### File 与对象存储 Sink

File Sink 写的是文件，因此不使用 `generate_sink_sql`、`query` 或数据库 upsert。

当前不同文件 connector 的支持边界如下：

| Connector | 是否暴露 Save Mode 参数 | 说明 |
|-----------|-------------------------|------|
| LocalFile | 是 | 处理本地目录和文件。 |
| HdfsFile | 是 | 处理 HDFS 目录和文件。 |
| FtpFile | 是 | 处理 FTP 目录和文件。 |
| SftpFile | 是 | 处理 SFTP 目录和文件。 |
| S3File | 是 | 通过 File Sink save mode 流程处理 S3 路径和对象。 |
| OssFile | 是 | 通过 File Sink save mode 流程处理 OSS 路径和对象。 |
| ObsFile | 否 | 当前 sink option rule 没有暴露 `schema_save_mode` 或 `data_save_mode`。 |
| CosFile | 否 | 当前 sink option rule 没有暴露 `schema_save_mode` 或 `data_save_mode`。 |
| BosFile | 否 | 当前 sink option rule 没有暴露 `schema_save_mode` 或 `data_save_mode`。 |

如果某个文件 connector 页面没有列出 `schema_save_mode` 或 `data_save_mode`，不要默认认为该 connector 可以接收这些参数。

## 示例

### JDBC 自动生成 SQL 并使用 Save Mode

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/sales"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "change_me"

    generate_sink_sql = true
    database = "sales"
    table = "public.orders"
    primary_keys = ["id"]

    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### JDBC 自定义 SQL，不执行 Save Mode

```hocon
sink {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/sales"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "change_me"

    query = "INSERT INTO orders(id, amount) VALUES (?, ?)"
  }
}
```

在这个模式下，JDBC Sink 只通过 `query` 写入每一行，不会执行 `schema_save_mode`、`data_save_mode` 或 `custom_sql`。

### S3File 写入前清空已有数据

```hocon
sink {
  S3File {
    path = "/warehouse/orders"
    bucket = "s3a://example-bucket"
    fs.s3a.endpoint = "s3.amazonaws.com"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "..."
    secret_key = "..."

    file_format_type = "json"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "DROP_DATA"
  }
}
```

## 故障排查

### 配了 `generate_sink_sql = true` 但仍然只是 INSERT

检查 SeaTunnel 是否拿到了可用 key。需要 upsert、update 或 delete 时，建议显式配置 `primary_keys`。

### JDBC Sink 的 `custom_sql` 没有执行

检查 Sink 是否配置了 `query`。JDBC 自定义 query 模式不会执行 save mode 处理，因此会跳过 `custom_sql`。

### File Sink 不接受 `data_save_mode`

检查具体 connector 参数表。`S3File`、`OssFile`、`HdfsFile`、`FtpFile`、`SftpFile`、`LocalFile` 暴露文件 save mode 参数；`ObsFile`、`CosFile` 和 `BosFile` 当前未暴露。

### 我只想建表，不想抽取数据

Save mode 是 Sink 作业写入前的一部分，SeaTunnel 目前没有通过 `schema_save_mode` 提供独立的“只执行 DDL”模式。如果作业没有数据，Sink 仍可能完成初始化，但这不能替代专门的 schema 管理流程。
