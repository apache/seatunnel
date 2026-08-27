import ChangeLog from '../changelog/connector-file-oss-jindo.md';

# OssJindoFile

> OssJindo file Sink 连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  默认情况下，我们使用 2PC commit 来确保“精确一次”。

- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

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

## 描述

使用 Jindo（阿里云 EMR）SDK 通过 HDFS 协议将数据写入阿里云 OSS 文件系统。

:::tip

您需要下载 [jindosdk-4.6.1.tar.gz](https://jindodata-binary.oss-cn-shanghai.aliyuncs.com/release/4.6.1/jindosdk-4.6.1.tar.gz)
然后解压缩，将 `jindo-sdk-4.6.1.jar` 和 `jindo-core-4.6.1.jar` 从 `lib` 复制到 `${SEATUNNEL_HOME}/lib`。

如果您使用 Spark/Flink，为了使用此连接器，您必须确保您的 Spark/Flink 集群已集成 Hadoop。测试的 Hadoop 版本是 2.x。

如果您使用 SeaTunnel Engine，当您下载并安装 SeaTunnel Engine 时会自动集成 Hadoop jar。您可以在 `${SEATUNNEL_HOME}/lib` 下检查 jar 包以确认这一点。

我们为了支持更多的文件类型做了一些权衡，因此使用 HDFS 协议对 OSS 进行内部访问，此连接器需要一些 Hadoop 依赖项。
它仅支持 Hadoop 版本 **2.9.X+**。

:::

## 数据库依赖

该连接器通过 Jindo SDK 访问阿里云 OSS。Jindo SDK 的 jar 包（`jindo-sdk-4.6.1.jar`、`jindo-core-4.6.1.jar`）必须放置在运行作业的每个节点的 `${SEATUNNEL_HOME}/lib` 下。

## Sink 选项

| 名称                                    | 类型      | 必需 | 默认值                                        | 描述                                                                                                                                          |
|---------------------------------------|---------|----|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | 是  | -                                          | Sink 写入的目标目录路径。如果目录不存在则会自动创建。                                                                                                                  |
| tmp_path                              | string  | 否  | /tmp/seatunnel                             | 结果文件将首先写入临时路径，然后使用 `mv` 将临时目录提交到目标目录。需要一个 OSS 目录。                                                                                          |
| bucket                                | string  | 是  | -                                          | OSS 文件系统的桶地址，例如 `oss://tyrantlucifer-image-bed`。                                                                                                  |
| access_key                            | string  | 是  | -                                          | OSS 桶的访问密钥。                                                                                                                                |
| access_secret                         | string  | 是  | -                                          | OSS 桶的访问密钥（密钥）。                                                                                                                            |
| endpoint                              | string  | 是  | -                                          | OSS 端点，例如 `oss-cn-beijing.aliyuncs.com`。                                                                                                       |
| custom_filename                       | boolean | 否  | false                                      | 是否需要自定义文件名。                                                                                                                                |
| file_name_expression                  | string  | 否  | "${transactionId}"                         | 仅在 `custom_filename` 为 `true` 时使用。                                                                                                          |
| filename_time_format                  | string  | 否  | "yyyy.MM.dd"                               | 仅在 `custom_filename` 为 `true` 时使用。                                                                                                          |
| file_format_type                      | string  | 否  | "csv"                                      | 文件格式类型，支持：`text`、`csv`、`parquet`、`orc`、`json`、`excel`、`xml`、`binary`、`canal_json`、`debezium_json`、`maxwell_json`。                          |
| filename_extension                    | string  | 否  | -                                          | 使用自定义文件扩展名覆盖默认扩展名，例如 `.xml`、`.json`、`dat`、`.customtype`。                                                                                          |
| field_delimiter                       | string  | 否  | '\001' for text and ',' for csv            | 仅当 `file_format_type` 为 `text` 和 `csv` 时使用。                                                                                                  |
| row_delimiter                         | string  | 否  | "\n"                                       | 仅当 `file_format_type` 为 `text`、`csv` 和 `json` 时使用。                                                                                          |
| have_partition                        | boolean | 否  | false                                      | 是否需要处理分区。                                                                                                                                  |
| partition_by                          | array   | 否  | -                                          | 仅在 `have_partition` 为 `true` 时使用。                                                                                                           |
| partition_dir_expression              | string  | 否  | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 仅在 `have_partition` 为 `true` 时使用。                                                                                                           |
| is_partition_field_write_in_file      | boolean | 否  | false                                      | 仅在 `have_partition` 为 `true` 时使用。                                                                                                           |
| sink_columns                          | array   | 否  |                                            | 当此参数为空时，所有字段都为 Sink 列。                                                                                                                       |
| is_enable_transaction                 | boolean | 否  | true                                       | 若为 `true`，写入目标目录的数据不会丢失或重复；当为 `true` 时，会自动在文件名前缀添加 `${transactionId}_`。                                                                       |
| batch_size                            | int     | 否  | 1000000                                    | 单个文件的最大行数。对于 SeaTunnel Engine，文件中的行数由 `batch_size` 和 `checkpoint.interval` 共同决定。                                                                       |
| compress_codec                        | string  | 否  | none                                       | 文件的压缩编解码器。Excel 格式不支持任何压缩格式。                                                                                                                 |
| common-options                        | object  | 否  | -                                          | Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md) 了解详情。                                                            |
| max_rows_in_memory                    | int     | 否  | -                                          | 仅当 `file_format_type` 为 `excel` 时使用。                                                                                                      |
| sheet_max_rows                        | int     | 否  | 1048576                                    | 仅当 `file_format_type` 为 `excel` 时使用；每个工作表允许写入的最大行数。                                                                                            |
| sheet_name                            | string  | 否  | Sheet${Random number}                      | 仅当 `file_format_type` 为 `excel` 时使用。                                                                                                      |
| csv_string_quote_mode                 | enum    | 否  | MINIMAL                                    | 仅当 `file_format_type` 为 `csv` 时使用。                                                                                                       |
| xml_root_tag                          | string  | 否  | RECORDS                                    | 仅当 `file_format_type` 为 `xml` 时使用。                                                                                                        |
| xml_row_tag                           | string  | 否  | RECORD                                     | 仅当 `file_format_type` 为 `xml` 时使用。                                                                                                        |
| xml_use_attr_format                   | boolean | 否  | -                                          | 仅当 `file_format_type` 为 `xml` 时使用。                                                                                                        |
| single_file_mode                      | boolean | 否  | false                                      | 每个并行度只输出一个文件。启用此选项后，`batch_size` 不再生效，输出文件名不包含文件块后缀。                                                                                          |
| create_empty_file_when_no_data        | boolean | 否  | false                                      | 当上游没有数据同步时，仍然会生成对应的数据文件。                                                                                                                     |
| parquet_avro_write_timestamp_as_int96 | boolean | 否  | false                                      | 仅当 `file_format_type` 为 `parquet` 时使用。                                                                                                    |
| parquet_avro_write_fixed_as_int96     | array   | 否  | -                                          | 仅当 `file_format_type` 为 `parquet` 时使用。                                                                                                    |
| encoding                              | string  | 否  | "UTF-8"                                    | 仅当 `file_format_type` 为 `json`、`text`、`csv`、`xml` 时使用。要写入文件的字符集，通过 `Charset.forName(encoding)` 解析。                                                  |
| merge_update_event                    | boolean | 否  | false                                      | 仅当 `file_format_type` 为 `canal_json`、`debezium_json`、`maxwell_json` 时使用。设置为 `true` 时，会将 `UPDATE_AFTER` 与 `UPDATE_BEFORE` 合并为 `UPDATE` 事件数据。                       |
| schema_evolution_enabled              | boolean | 否  | false                                      | 开启 Schema 演变支持，适用于 CDC 管道。为 `true` 时，来自上游的 `ADD/DROP/RENAME/MODIFY` 列事件无需重启作业即可应用到 Sink。不支持 `binary` 格式。                                                  |

### path [string]

Sink 写入的目标目录路径。如果目录不存在则会自动创建。

### bucket [string]

OSS 文件系统的桶地址，例如：`oss://tyrantlucifer-image-bed`。

### access_key [string]

OSS 桶的访问密钥。

### access_secret [string]

OSS 桶的访问密钥（密钥）。

### endpoint [string]

OSS 端点，例如 `oss-cn-beijing.aliyuncs.com`。

### custom_filename [boolean]

是否自定义文件名。

### file_name_expression [string]

仅在 `custom_filename` 为 `true` 时使用。

`file_name_expression` 描述将在 `path` 下创建的文件名表达式。可以在其中加入变量 `${now}` 或 `${uuid}`，例如 `test_${uuid}_${now}`，`${now}` 表示当前时间，其格式可以通过 `filename_time_format` 指定。

请注意，如果 `is_enable_transaction` 为 `true`，会自动在文件名前缀添加 `${transactionId}_`。

### filename_time_format [string]

仅在 `custom_filename` 为 `true` 时使用。

当 `file_name_expression` 参数中包含 `xxxx-${now}` 时，`filename_time_format` 用于指定路径的时间格式，默认值为 `yyyy.MM.dd`。常用时间格式如下：

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

### file_format_type [string]

支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `canal_json` `debezium_json` `maxwell_json`

请注意，最终文件名将以 `file_format_type` 的后缀结尾，文本文件的后缀为 `txt`。

### field_delimiter [string]

数据行中列之间的分隔符。仅当 `file_format_type` 为 `text` 和 `csv` 时使用。

### row_delimiter [string]

文件中行之间的分隔符。仅当 `file_format_type` 为 `text`、`csv` 和 `json` 时使用。

### have_partition [boolean]

是否需要处理分区。

### partition_by [array]

仅在 `have_partition` 为 `true` 时使用。

根据所选字段对数据进行分区。

### partition_dir_expression [string]

仅在 `have_partition` 为 `true` 时使用。

如果指定了 `partition_by`，会根据分区信息生成对应的分区目录，最终文件会写入该分区目录下。

默认 `partition_dir_expression` 为 `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`，其中 `k0` 是第一个分区字段，`v0` 是第一个分区字段的值。

### is_partition_field_write_in_file [boolean]

仅在 `have_partition` 为 `true` 时使用。

若为 `true`，分区字段及其值会写入数据文件中。

例如，如果要写 Hive 数据文件，应设为 `false`。

### sink_columns [array]

需要写入文件的列，默认值为来自 `Transform` 或 `Source` 的所有列。字段顺序决定文件的实际写入顺序。

### is_enable_transaction [boolean]

如果 `is_enable_transaction` 为 `true`，会保证写入目标目录的数据不丢失、不重复。

请注意，如果 `is_enable_transaction` 为 `true`，会自动在文件名前缀添加 `${transactionId}_`。

当前仅支持 `true`。

### batch_size [int]

单个文件的最大行数。对于 SeaTunnel Engine，文件中的行数由 `batch_size` 和 `checkpoint.interval` 共同决定。如果 `checkpoint.interval` 足够大，Sink Writer 会持续写入直到行数超过 `batch_size`；若 `checkpoint.interval` 较小，则每次 checkpoint 触发时会创建新文件。

### compress_codec [string]

文件的压缩编解码器，支持情况如下：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

提示：excel 格式不支持任何压缩格式。

### common options

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md) 了解详情。

### max_rows_in_memory [int]

当文件格式为 Excel 时，内存中可缓存的最大数据项数量。

### sheet_max_rows [int]

当文件格式为 Excel 时，每个工作表允许写入的最大行数。

### sheet_name [string]

写入工作簿的工作表名称。

### csv_string_quote_mode [string]

当文件格式为 CSV 时，CSV 的字符串引用模式。

- ALL：所有字符串字段都会被引用。
- MINIMAL：仅引用包含特殊字符（如字段分隔符、引号字符或行分隔符中任意字符）的字段。
- NONE：永不引用字段。当数据中出现分隔符时，打印机会在其前添加转义字符；若未设置转义字符，格式校验会抛出异常。

### xml_root_tag [string]

指定 XML 文件中根元素的标签名。

### xml_row_tag [string]

指定 XML 文件中数据行的标签名。

### xml_use_attr_format [boolean]

指定是否使用标签属性格式处理数据。

### parquet_avro_write_timestamp_as_int96 [boolean]

支持从时间戳写入 Parquet INT96，仅对 parquet 文件有效。

### parquet_avro_write_fixed_as_int96 [array]

支持从 12 字节字段写入 Parquet INT96，仅对 parquet 文件有效。

### encoding [string]

仅当 `file_format_type` 为 `json`、`text`、`csv`、`xml` 时使用。指定要写入文件的字符集，通过 `Charset.forName(encoding)` 解析。

### merge_update_event [boolean]

仅当 `file_format_type` 为 `canal_json`、`debezium_json`、`maxwell_json` 时使用。设置为 `true` 时，会将 `UPDATE_AFTER` 与 `UPDATE_BEFORE` 合并为 `UPDATE` 事件数据。

### schema_evolution_enabled [boolean]

设置为 `true` 时，文件 Sink 可在运行时处理 CDC Schema 变更事件（ADD COLUMN、DROP COLUMN、RENAME COLUMN、MODIFY COLUMN 类型），无需重启作业。每次 Schema 变更时，当前输出文件会被关闭，并以新 Schema 打开一个新文件。

**支持的格式：** 除 `binary` 外的所有文件格式。将此选项与 `file_format_type = binary` 一起使用时，作业启动时会抛出配置校验错误。

**分区约束：** 当 `have_partition = true` 时，不允许删除 `partition_by` 中列出的列，违反时会立即抛出异常。分区列在 Schema 变更过程中必须保持稳定。

**当 `schema_evolution_enabled = false`（默认值）时：** 若上游 CDC Source 配置了 `schema-changes.enabled = true` 且 Sink 收到 `AlterTableEvent`，作业会立即抛出如下错误：
> `Received AlterTableEvent but schema_evolution_enabled=false at this sink. Either set schema_evolution_enabled=true to handle schema changes, or set schema-changes.enabled=false at the CDC source to suppress them.`

使用默认 CDC Source 配置（`schema-changes.enabled = false`）的用户不受影响。

**已知限制：** Schema 变更与 Checkpoint 不是原子操作。若作业在文件轮转与 Schema 元数据更新之间的窗口期崩溃，恢复后写入的数据行可能使用变更前的 Schema。这是与其他 SeaTunnel Sink 共同存在的已知架构限制。完整的重启后 DDL 正确性支持需要配套的 CDC Source 修复（另行跟踪）。

CDC 管道中的使用示例：

```hocon
OssJindoFile {
    path = "/tmp/cdc/${table_name}"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "parquet"
    schema_evolution_enabled = true
    have_partition = true
    partition_by = ["updated_at_month"]
}
```

## 例子

适用于具有 `have_partition`、`custom_filename` 和 `sink_columns` 的文本文件格式：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  OssJindoFile {
    path="/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
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
    sink_columns = ["name","age"]
    is_enable_transaction = true
  }
}
```

适用于带 `sink_columns` 的 parquet 文件格式：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  OssJindoFile {
    path = "/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }
}
```

对于 orc 文件格式的简单配置：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  OssJindoFile {
    path="/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
  }
}
```

适用于带 `merge_update_event` 的 canal_json 格式（合并 CDC 更新事件）：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  OssJindoFile {
    path = "/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "canal_json"
    merge_update_event = true
  }
}
```

## 变更日志

<ChangeLog />
