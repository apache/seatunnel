import ChangeLog from '../changelog/connector-file-sftp.md';

# SftpFile

> Sftp File Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 SFTP 将数据写入远程目录。连接器支持多种文件格式（`text`、`csv`、`parquet`、`orc`、
`json`、`excel`、`xml`、`binary`、`canal_json`、`debezium_json`、`maxwell_json`），支持按
字段分区、自定义文件名以及 CDC 管道运行时的结构变更。

:::tip

如果使用 Spark/Flink，请确保集群已集成 Hadoop，测试版本为 Hadoop 2.x。

如果使用 SeaTunnel Engine，安装包中已包含 Hadoop JAR，可在 `${SEATUNNEL_HOME}/lib` 下确认。

连接器同时支持密码认证（`password`）和公钥认证（`keyfile`）。

:::

## 主要特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读写任意格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  默认使用 2PC 提交保证 `精确一次`。

- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

  在 `path` 中使用 `${database_name}` 与 `${table_name}` 占位符，把不同上游表的数据路由到不同的输出目录。

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] canal_json
  - [x] debezium_json
  - [x] maxwell_json

## 参数

| 名称                                    | 类型      | 是否必填 | 默认值                                          | 描述                                                                                                                                                |
|---------------------------------------|---------|------|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | string  | 是    | -                                            | 目标 SFTP 服务器地址。                                                                                                                                |
| port                                  | int     | 是    | -                                            | 目标 SFTP 端口，通常为 `22`。                                                                                                                          |
| user                                  | string  | 是    | -                                            | SFTP 登录用户名。                                                                                                                                    |
| password                              | string  | 否    | -                                            | SFTP 登录密码。未配置 `keyfile` 时需要配置。                                                                                                              |
| keyfile                               | string  | 否    | -                                            | 用于 SFTP 公钥认证的私钥文件路径。                                                                                                                       |
| path                                  | string  | 是    | -                                            | SFTP 服务端的目标目录。                                                                                                                                |
| tmp_path                              | string  | 是    | /tmp/seatunnel                               | SFTP 服务端的临时目录；写入时会先把输出文件写到这里，检查点完成后再 `mv` 到 `path`。                                                                              |
| custom_filename                       | boolean | 否    | false                                        | 是否需要自定义文件名。                                                                                                                                  |
| file_name_expression                  | string  | 否    | "${transactionId}"                           | 仅在 `custom_filename = true` 时使用。                                                                                                                |
| filename_time_format                  | string  | 否    | "yyyy.MM.dd"                                 | 仅在 `custom_filename = true` 时使用。                                                                                                                |
| file_format_type                      | string  | 否    | "csv"                                        | 支持 `text`、`csv`、`parquet`、`orc`、`json`、`excel`、`xml`、`binary`、`canal_json`、`debezium_json`、`maxwell_json`。                                          |
| filename_extension                    | string  | 否    | -                                            | 用自定义后缀覆盖默认后缀，例如 `.xml`、`.json`、`dat`、`.customtype`。                                                                                       |
| field_delimiter                       | string  | 否    | '\001' for text and ',' for csv              | 列分隔符，仅在 `file_format_type` 为 `text` 或 `csv` 时生效。                                                                                             |
| row_delimiter                         | string  | 否    | "\n"                                         | 行分隔符，仅在 `file_format_type` 为 `text`、`csv` 或 `json` 时生效。                                                                                      |
| have_partition                        | boolean | 否    | false                                        | 是否按上游字段值分区输出。                                                                                                                              |
| partition_by                          | array   | 否    | -                                            | 仅在 `have_partition = true` 时使用。                                                                                                                  |
| partition_dir_expression              | string  | 否    | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"   | 仅在 `have_partition = true` 时使用。                                                                                                                  |
| is_partition_field_write_in_file      | boolean | 否    | false                                        | 仅在 `have_partition = true` 时使用。                                                                                                                  |
| sink_columns                          | array   | 否    |                                              | 实际写入文件的列；为空时写入全部上游列。                                                                                                                    |
| is_enable_transaction                 | boolean | 否    | true                                         | 预留参数，目前仅支持 `true`（也是默认行为）。                                                                                                              |
| batch_size                            | int     | 否    | 1000000                                      | 单个文件最大行数，达到后滚动生成新文件。                                                                                                                    |
| compress_codec                        | string  | 否    | none                                         | 压缩编码，详见下方说明。                                                                                                                                |
| common-options                        | object  | 否    | -                                            | Sink 插件通用参数，详情请参考 [Sink Common Options](../common-options/sink-common-options.md)。                                                          |
| max_rows_in_memory                    | int     | 否    | -                                            | 仅在 `file_format_type` 为 `excel` 时使用。                                                                                                            |
| sheet_max_rows                        | int     | 否    | 1048576                                      | 仅在 `file_format_type` 为 `excel` 时使用。                                                                                                            |
| sheet_name                            | string  | 否    | Sheet${Random number}                        | 仅在 `file_format_type` 为 `excel` 时使用。                                                                                                            |
| csv_string_quote_mode                 | enum    | 否    | MINIMAL                                      | 仅在 `file_format_type` 为 `csv` 时使用。                                                                                                              |
| xml_root_tag                          | string  | 否    | RECORDS                                      | 仅在 `file_format_type` 为 `xml` 时使用。                                                                                                              |
| xml_row_tag                           | string  | 否    | RECORD                                       | 仅在 `file_format_type` 为 `xml` 时使用。                                                                                                              |
| xml_use_attr_format                   | boolean | 否    | -                                            | 仅在 `file_format_type` 为 `xml` 时使用。                                                                                                              |
| single_file_mode                      | boolean | 否    | false                                        | 每个并行度只输出一个文件；开启后 `batch_size` 不再生效，文件名也不带分块后缀。                                                                              |
| create_empty_file_when_no_data        | boolean | 否    | false                                        | 上游没有数据时仍生成对应的空文件。                                                                                                                        |
| parquet_avro_write_timestamp_as_int96 | boolean | 否    | false                                        | 仅在 `file_format_type` 为 `parquet` 时使用。                                                                                                          |
| parquet_avro_write_fixed_as_int96     | array   | 否    | -                                            | 仅在 `file_format_type` 为 `parquet` 时使用。                                                                                                          |
| enable_header_write                   | boolean | 否    | false                                        | 仅在 `file_format_type` 为 `text` 或 `csv` 时使用；为 true 时写入表头行。                                                                                |
| encoding                              | string  | 否    | "UTF-8"                                      | 仅在 `file_format_type` 为 `json`、`text`、`csv` 或 `xml` 时使用。                                                                                     |
| schema_evolution_enabled              | boolean | 否    | false                                        | 启用 CDC 结构变更（ADD/DROP/RENAME/MODIFY）支持，无需重启任务；`binary` 不支持，详见下方说明。                                                            |
| schema_save_mode                      | string  | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST                 | 任务启动时对目标目录的处理方式。                                                                                                                          |
| data_save_mode                        | string  | 否    | APPEND_DATA                                  | 任务启动时对目录中已有数据文件的处理方式。                                                                                                                  |
| merge_update_event                    | boolean | 否    | false                                        | 仅在 `file_format_type` 为 `canal_json`、`debezium_json` 或 `maxwell_json` 时使用。为 true 时，UPDATE_AFTER 和 UPDATE_BEFORE 会合并为单个 UPDATE 事件。    |

### file_name_expression [string]

仅在 `custom_filename = true` 时使用。

`file_name_expression` 描述 `path` 内创建文件的命名规则。支持在表达式中嵌入 `${now}` 或
`${uuid}`，例如 `test_${uuid}_${now}`。`${now}` 表示当前时间，时间格式可通过
`filename_time_format` 指定。

注意：当 `is_enable_transaction = true` 时，文件名会自动加上前缀 `${transactionId}_`。

### compress_codec [string]

输出文件的压缩编码，支持以下组合：

- `text` / `json` / `csv`：`lzo`、`none`
- `orc`：`lzo`、`snappy`、`lz4`、`zlib`、`none`
- `parquet`：`lzo`、`snappy`、`lz4`、`gzip`、`brotli`、`zstd`、`none`

`excel` 不支持任何压缩编码。

### schema_save_mode [string]

任务启动时对目标目录的处理方式：

- `CREATE_SCHEMA_WHEN_NOT_EXIST`（默认）：目录不存在时创建，存在则跳过。
- `RECREATE_SCHEMA`：目录存在时先删除再重建。
- `ERROR_WHEN_SCHEMA_NOT_EXIST`：目录不存在时立即报错。
- `IGNORE`：不做任何处理。

### data_save_mode [string]

任务启动时对目录中已有数据文件的处理方式：

- `APPEND_DATA`（默认）：保留目录与已有文件，继续追加。
- `DROP_DATA`：保留目录，删除已有数据文件。
- `ERROR_WHEN_DATA_EXISTS`：目录下存在数据文件时立即报错。

### schema_evolution_enabled [boolean]

当设置为 `true` 时，SFTP 文件 Sink 会在不重启任务的情况下处理上游 CDC 结构变更事件
（ADD COLUMN、DROP COLUMN、RENAME COLUMN、MODIFY COLUMN）。每次结构变更会先关闭当前输出
文件，再用新结构打开新文件。

**支持的格式：** 除 `binary` 外的全部文件格式。与 `file_format_type = binary` 同时启用会在
任务启动时因配置校验失败。

**分区约束：** 当 `have_partition = true` 时，不允许 DROP `partition_by` 中列出的字段，
否则会立即报错。分区列在结构变更前后必须保持稳定。

**当 `schema_evolution_enabled = false`（默认）时：** 如果上游 CDC 源开启了
`schema-changes.enabled = true` 并且有 `AlterTableEvent` 进入 Sink，任务会立即失败并给出明确
错误：

> `Received AlterTableEvent but schema_evolution_enabled=false at this sink. Either set schema_evolution_enabled=true to handle schema changes, or set schema-changes.enabled=false at the CDC source to suppress them.`

使用默认 CDC 源配置（`schema-changes.enabled = false`）的用户完全不受影响。

**已知限制：** 结构变更与检查点不是原子的。如果任务恰好在文件滚动与结构元数据更新之间的
短暂窗口崩溃，恢复后写入的行可能仍使用变更前的结构。该架构性限制与其他 SeaTunnel Sink 相同。

## 示例

### 带分区与自定义文件名的 Text 格式

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    password = "********"
    path = "/data/sftp/seatunnel/job1"
    tmp_path = "/data/sftp/seatunnel/tmp"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    sink_columns = ["name", "age"]
    is_enable_transaction = true
  }
}
```

### 多表写入

当上游覆盖多张表，并且希望每张表落到各自目录时，可在 `path` 中使用 `${table_name}` 占位符。

```hocon
sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    password = "********"
    path = "/data/sftp/seatunnel/job1/${table_name}"
    tmp_path = "/data/sftp/seatunnel/tmp"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    sink_columns = ["name", "age"]
    is_enable_transaction = true
    schema_save_mode = "RECREATE_SCHEMA"
    data_save_mode = "DROP_DATA"
  }
}
```

### CDC 结构变更

```hocon
sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    password = "********"
    path = "/data/sftp/cdc/${table_name}"
    tmp_path = "/data/sftp/cdc/tmp"
    file_format_type = "parquet"
    schema_evolution_enabled = true
    have_partition = true
    partition_by = ["updated_at_month"]
  }
}
```

### Parquet + 公钥认证

```hocon
sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    keyfile = "/home/seatunnel/.ssh/id_rsa"
    path = "/data/sftp/seatunnel/parquet"
    tmp_path = "/data/sftp/seatunnel/tmp"
    file_format_type = "parquet"
    sink_columns = ["name", "age"]
  }
}
```

## 变更日志

<ChangeLog />