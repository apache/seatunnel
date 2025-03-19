import ChangeLog from '../changelog/connector-file-ftp.md';

# FtpFile

> Ftp文件数据接收器连接器

## 描述

将数据输出到FTP。

:::提示

如果你使用Spark或Flink，为了使用这个连接器，你必须确保你的Spark或Flink集群已经集成了Hadoop。经测试的Hadoop版本是2.x版本。 

如果你使用SeaTunnel引擎，在你下载并安装SeaTunnel引擎时，它会自动集成Hadoop的jar包。你可以查看${SEATUNNEL_HOME}/lib目录下的jar包来确认这一点。  

:::

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)

  默认情况下，我们使用两阶段提交（2PC）来确保`精确一次`

- [x] 文件格式
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## 选项
| 名称                                  | 类型    | 是否必须 | 默认值                                    | 描述                                                                                                                                                            |
|---------------------------------------|---------|------------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | string  | 是       | -                                          |                                                              |
| port                                  | int     | 是        | -                                          |                                                              |
| user                                  | string  | 是        | -                                          |                                                              |
| password                              | string  | 是        | -                                          |                                                              |
| path                                  | string  | 是        | -                                          |                                                                                                                                   |
| tmp_path                              | string  | 是        | /tmp/seatunnel                             | 结果文件将首先写入一个临时路径，然后使用 `mv` 命令将临时目录提交到目标目录。需要是一个FTP目录。                                                      |
| connection_mode                       | string  | 否        | active_local                               | 目标FTP连接模式                                                                                                                                         |
| custom_filename                       | boolean | 否        |  false                                      | 是否需要自定义文件名                                                                                                                                   |
| file_name_expression                  | string  | 否        | "${transactionId}"                         | 仅在 `custom_filename` 为 `true` 时使用                                                                                                                                 |
| filename_time_format                  | string  | 否        | "yyyy.MM.dd"                               | 仅在 `custom_filename` 为 `true` 时使用                                                                                                                                 |
| file_format_type                      | string  | 否        | "csv"                                      |                                                              |
| filename_extension                    | string  | 否        | -                                          | 用自定义的文件扩展名覆盖默认的文件扩展名。例如：`.xml`、`.json`、`dat`、`.customtype`                                                 |
| field_delimiter                       | string  | 否        | '\001'                                     | 仅在 `file_format_type` 为 `text` 时使用                                                                                                                                |
| row_delimiter                         | string  | 否        | "\n"                                       | 仅在 `file_format_type` 为 `text` 时使用                                                                                                                                |
| have_partition                        | boolean | 否        | false                                      | 是否需要处理分区。                                                                                                                                |
| partition_by                          | array   | 否        | -                                          | 仅在 `have_partition` 为 `true` 时使用                                                                                                                                  |
| partition_dir_expression              | string  | 否        | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 仅在 `have_partition` 为 `true` 时使用                                                                                                                                  |
| is_partition_field_write_in_file      | boolean | 否        | false                                      | 仅在 `have_partition` 为 `true` 时使用                                                                                                                                  |
| sink_columns                          | array   | 否        |                                            | 当此参数为空时，所有字段都是要写入的列                                                                                                              |
| is_enable_transaction                 | boolean | 否        | true                                       |                                                              |
| batch_size                            | int     | 否        | 1000000                                    |                                                              |
| compress_codec                        | string  | 否        | none                                       |                                                              |
| common-options                        | object  | 否        | -                                          |                                                              |
| max_rows_in_memory                    | int     | 否        | -                                          | 仅在 `file_format_type` 为 `excel` 时使用。                                                                                                                              |
| sheet_name                            | string  | 否        | Sheet${随机数}                      | 仅在 `file_format_type` 为 `excel` 时使用。                                                                                                                              |
| csv_string_quote_mode                 | enum    | 否        | MINIMAL                                    | 仅在 `file_format` 为 `csv` 时使用。                                                                                                                                     |
| xml_root_tag                          | string  | 否        | RECORDS                                    | 仅在 `file_format` 为 `xml` 时使用。                                                                                                                                     |
| xml_row_tag                           | string  | 否        | RECORD                                     | 仅在 `file_format` 为 `xml` 时使用。                                                                                                                                     |
| xml_use_attr_format                   | boolean | 否        | -                                          | 仅在 `file_format` 为 `xml` 时使用。                                                                                                                                     |
| single_file_mode                      | boolean | 否        | false                                      | 每个并行处理只会输出一个文件。当此参数开启时，`batch_size` 将不会生效。输出文件名不会有文件分块后缀。 |
| create_empty_file_when_no_data        | boolean | 否        | false                                      | 当上游没有数据同步时，仍然会生成相应的数据文件。                                                                      |
| parquet_avro_write_timestamp_as_int96 | boolean | 否        | false                                      | 仅在 `file_format` 为 `parquet` 时使用。                                                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | 否        | -                                          | 仅在 `file_format` 为 `parquet` 时使用。                                                                                                                                 |
| enable_header_write                   | boolean | 否        | false                                      | 仅在 `file_format_type` 为 `text`、`csv` 时使用。<br/> `false`：不写入表头，`true`：写入表头。                                                                          |
| encoding                              | string  | 否        | "UTF-8"                                    | 仅在 `file_format_type` 为 `json`、`text`、`csv`、`xml` 时使用。                                                                                                                  |
| schema_save_mode                      | string  | 否        | CREATE_SCHEMA_WHEN_NOT_EXIST               | 现有目录处理方法                                                                                                                                         |
| data_save_mode                        | string  | 否       | APPEND_DATA                                | 现有数据处理方法                                                                                                                                        |


### host [string]

目标FTP主机是必需的。

### port [int]

目标FTP端口是必需的。

### user [string]

目标FTP用户名是必需的。

### password [string]

目标FTP密码是必需的。

### path [string]

目标目录路径是必需的。

### connection_mode [string]

目标 FTP 连接模式是必需的，默认值为主动模式，支持以下几种模式：

`active_local`（本地主动模式） `passive_local`（本地被动模式）

### custom_filename [boolean]

是否自定义文件名

### file_name_expression [string]

仅当 `custom_filename`为 `true`时使用。 

`file_name_expression`描述了将在 `path`中创建的文件表达式。我们可以在 `file_name_expression` 中添加变量 `${now}`或 `${uuid}`，例如 `test_${uuid}_${now}` 。

 `${now}` 表示当前时间，其格式可以通过指定选项 `filename_time_format`来定义。 

请注意，如果 `is_enable_transaction`为 `true`，我们将自动在文件名的开头添加 `${transactionId}_`。 

### filename_time_format [string]

仅当 `custom_filename`为 `true`时才会用到。

当 `file_name_expression` 参数中的格式为 `xxxx-${now}` 时，`filename_time_format` 可以指定路径的时间格式，其默认值为 `yyyy.MM.dd` 。常用的时间格式列举如下：

| **代表符号** | 描述               |
| ------------ | ------------------ |
| y            | Year               |
| M            | Month              |
| d            | Day of month       |
| H            | Hour in day (0-23) |
| m            | Minute in hour     |
| s            | Second in minute   |

### file_format_type [string]

我们支持以下文件类型： 

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` 

请注意，最终的文件名将会以 `file_format_type` 的后缀结尾，文本文件的后缀是 `txt`。 

### field_delimiter [string]

一行数据中各列之间的分隔符。仅 `text`文件格式需要用到。 

### row_delimiter [string]

一行数据中各列之间的分隔符。仅在`text`文件格式中需要用到。 

### have_partition [boolean]

你是否需要对分区进行处理。 

### partition_by [array]

仅在 `have_partition` 为 `true` 时才使用。 

根据选定的字段对数据进行分区。

### partition_dir_expression [string]

仅在 `have_partition` 为 `true` 时使用。

 若指定了 `partition_by`，我们会根据分区信息生成相应的分区目录，最终文件将被放置在该分区目录中。 

默认的 `partition_dir_expression` 为 `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`。其中，`k0` 是第一个分区字段，`v0` 是第一个分区字段的值。 

### is_partition_field_write_in_file [boolean]

仅在 `have_partition` 为 `true` 时使用。 

如果 `is_partition_field_write_in_file` 为 `true`，那么分区字段及其对应的值将被写入数据文件中。 

例如，如果你想要写入一个 Hive 数据文件，该值（`is_partition_field_write_in_file`）应该设为 `false`。 

### sink_columns [array]

哪些列需要写入文件，默认值是从 `Transform` 或 `Source` 获取的所有列。 

字段的顺序决定了实际写入文件时的顺序。 

### is_enable_transaction [boolean]

如果 `is_enable_transaction`为 `true`），我们将确保在数据写入目标目录时不会丢失或重复。 

请注意，如果 `is_enable_transaction` 为 `true`，我们将自动在文件名开头添加 `${transactionId}_`。 

目前仅支持 `true`这一选项。 

### batch_size [int]

一个文件中的最大行数。对于 SeaTunnel 引擎，文件中的行数由 `batch_size` 和 `checkpoint.interval` 共同决定。如果 `checkpoint.interval` 的值足够大，sink writer 会向一个文件中写入行，直到文件中的行数超过 `batch_size`。如果 `checkpoint.interval` 较小，当新的检查点触发时，sink writer 会创建一个新文件。 

### compress_codec [string]

文件的压缩编解码器及其所支持的详细情况如下： 

文件的压缩编解码器以及所支持的详细信息如下所示：

- txt：`lzo`  `none`

- json：`lzo`  `none` 

- csv：`lzo`  `none` 

- orc：`lzo`  `snappy`  `lz4`  `zlib`  `none`  

- parquet：`lzo`  `snappy`  `lz4`  `gzip`  `brotli`  `zstd`  `none` ` 

  提示：Excel 类型不支持任何压缩格式。 

### common options

Sink 插件的通用参数，请参考[Sink通用选项](../sink-common-options.md)了解详细信息。 

### max_rows_in_memory [int]

当文件格式为Excel时，可在内存中缓存的数据项的最大数量。 

### sheet_name [string]

写入工作簿的工作表。

### csv_string_quote_mode [string]

当文件格式为CSV时，CSV的字符串引号模式： 

- ALL（全部）：所有字符串字段都将被加上引号。 
- MINIMAL（最少）：仅对包含特殊字符（如字段分隔符、引号字符或行分隔字符串中的任何字符）的字段加上引号。
- NONE（无）：从不对字段加引号。当数据中出现分隔符时，打印程序会在其前面加上转义字符。如果未设置转义字符，格式验证将抛出异常。 

### xml_root_tag [string]

指定 XML 文件中根元素的标签名称。

### xml_row_tag [string]

指定 XML 文件中数据行的标签名称。

### xml_use_attr_format [boolean]

指定是否使用标签属性格式来处理数据。 

### parquet_avro_write_timestamp_as_int96 [boolean]

支持从时间戳写入 Parquet 格式的 INT96 类型数据，仅对 Parquet 文件有效。 

### parquet_avro_write_fixed_as_int96 [array]

支持从一个12字节的字段写入Parquet的INT96类型数据，仅对Parquet文件有效。 

### enable_header_write [boolean]

仅当文件格式类型为文本或CSV时使用。 false：不写入表头，true：写入表头。 

### encoding [string]

仅当文件格式类型为JSON、文本、CSV、XML时才使用。 

要写入的文件的编码。此参数将由 `Charset.forName(encoding)` 方法进行解析。 

### schema_save_mode [string]

现有目录处理方法：

- RECREATE_SCHEMA（重新创建模式）：目录不存在时创建；目录存在时，删除并重新创建。
- CREATE_SCHEMA_WHEN_NOT_EXIST（不存在时创建模式）：目录不存在时创建；目录存在时跳过处理
- ERROR_WHEN_SCHEMA_NOT_EXIST（模式不存在时出错）：目录不存在时报告错误。 
- IGNORE（忽略）：忽略对该表的处理。 

### data_save_mode [string]

现有数据处理方法：
- DROP_DATA（删除数据）：保留目录，删除数据文件。
- APPEND_DATA（追加数据）：保留目录和数据文件。
- ERROR_WHEN_DATA_EXISTS（数据存在时报错）：当存在数据文件时，报告错误。

## 示例

对于文本文件格式的简易配置 

```bash

FtpFile {
    host = "xxx.xxx.xxx.xxx"
    port = 21
    user = "username"
    password = "password"
    path = "/data/ftp"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    sink_columns = ["name","age"]
}

```

对于带有 `have_partition`、`custom_filename` 和 `sink_columns` 的文本文件格式 

```bash

FtpFile {
    host = "xxx.xxx.xxx.xxx"
    port = 21
    user = "username"
    password = "password"
    path = "/data/ftp/seatunnel/job1"
    tmp_path = "/data/ftp/seatunnel/tmp"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    sink_columns = ["name","age"]
    filename_time_format = "yyyy.MM.dd"
}

```

当我们的数据源端是多个表，并且希望将不同的数据按照不同的表达式存储到不同的目录时，我们可以按照这种方式进行配置。  

```hocon

FtpFile {
    host = "xxx.xxx.xxx.xxx"
    port = 21
    user = "username"
    password = "password"
    path = "/data/ftp/seatunnel/job1/${table_name}"
    tmp_path = "/data/ftp/seatunnel/tmp"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    sink_columns = ["name","age"]
    filename_time_format = "yyyy.MM.dd"
    schema_save_mode=RECREATE_SCHEMA
    data_save_mode=DROP_DATA
}

```

## 变更日志

<ChangeLog />