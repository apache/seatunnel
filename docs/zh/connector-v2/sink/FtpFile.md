# FtpFile

> Ftp 文件接收器

## 描述

将数据输出到 Ftp。

:::提示

如果你使用的是 spark/flink，为了使用此连接器，你必须确保你的 spark/flink 集群已集成 hadoop。已测试的 hadoop 版本是 2.x。

如果你使用 SeaTunnel Engine，它会在下载和安装 SeaTunnel Engine 时自动集成 hadoop jar。你可以在 ${SEATUNNEL_HOME}/lib 下检查 jar 包以确认这一点。

:::

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)

默认情况下，我们使用 2PC 提交以确保`精确一次`。

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

| 名称                                  | 类型    | 是否必传 | 默认值                                    | 描述                                                                                                                                                            |
|---------------------------------------|---------|----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | string  | 是      | -                                          |                                                                                                                                                                        |
| port                                  | int     | 是      | -                                          |                                                                                                                                                                        |
| user                                  | string  | 是      | -                                          |                                                                                                                                                                        |
| password                              | string  | 是      | -                                          |                                                                                                                                                                        |
| path                                  | string  | 是      | -                                          | 目标目录路径                                                                                                                                                                       |
| tmp_path                              | string  | 是      | /tmp/seatunnel                             | 结果文件将首先写入临时路径，然后使用 `mv` 将临时目录提交到目标目录。需要一个FTP目录。                                                      |
| connection_mode                       | string  | 否       | active_local                               | 目标ftp连接模式                                                                                                                                         |
| custom_filename                       | boolean | 否       | false                                     | 是否需要自定义文件名                                                                                                                                   |
| file_name_expression                  | string  | 否       | "${transactionId}"                         | 仅在 custom_filename 为true时使用                                                                                                                                 |
| filename_time_format                  | string  | 否       | "yyyy.MM.dd"                               | 仅在 custom_filename 为true时使用                                                                                                                                 |
| file_format_type                      | string  | 否       | "csv"                                      |  文件格式类                                                                                                                                                                      |
| field_delimiter                       | string  | 否       | '\001'                                     | 仅当 file_format_type 为text 时使用                                                                                                                                |
| row_delimiter                         | string  | 否       | "\n"                                       | 仅当 file_format_type 为text 时使用                                                                                                                                |
| have_partition                        | boolean | 否       | false                                      | 是否需要处理分区。                                                                                                                                |
| partition_by                          | array   | 否       | -                                          | 只有在 have_partition 为 true 时才使用                                                                                                                                  |
| partition_dir_expression              | string  | 否       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 只有在 have_partition为 true 时才使用                                                                                                                                  |
| is_partition_field_write_in_file      | boolean | 否       | false                                      | 只有在 have_partition为 true 时才使用                                                                                                                                  |
| sink_columns                          | array   | 否       |                                            | 当此参数为空时，所有字段都是 sink 列                                                                                                              |
| is_enable_transaction                 | boolean | 否       | true                                       |  是否启用事务                                                                                                                                                                      |
| batch_size                            | int     | 否       | 1000000                                    | 批量大小                                                                                                                                                                       |
| compress_codec                        | string  | 否       | none                                       | 压缩编码                                                                                                                                                                       |
| common-options                        | object  | 否       | -                                          |  常见选项                                                                                                                                                                      |
| max_rows_in_memory                    | int     | 否       | -                                          | 仅当 file_format_type 为 excel 时使用。                                                                                                                              |
| sheet_name                            | string  | 否       | Sheet${Random number}                      | 仅当 file_format_type 为 excel 时使用。                                                                                                                              |
| csv_string_quote_mode                 | enum    | 否       | MINIMAL                                    | 仅在 file_format 为 csv 时使用。                                                                                                                                     |
| xml_root_tag                          | string  | 否       | RECORDS                                    | 仅在 file_format 为 xml 时使用                                                                                                                                     |
| xml_row_tag                           | string  | 否       | RECORD                                     | 仅在 file_format为 xml 时使用                                                                                                                                     |
| xml_use_attr_format                   | boolean | 否       | -                                          | 仅在 file_format 为 xml 时使用                                                                                                                                     |
| single_file_mode                      | boolean | 否       | false                                      | 每个并行处理只会输出一个文件。启用此参数后，batch_size将不会生效。输出文件名没有文件块后缀。 |
| create_empty_file_when_no_data        | boolean | 否       | false                                      | 当上游没有数据同步时，仍然会生成相应的数据文件。                                                                      |
| parquet_avro_write_timestamp_as_int96 | boolean | 否       | false                                      | 仅在 file_format 为 parquet 时使用。                                                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | 否       | -                                          | 仅在 file_format 为 parquet 时使用。                                                                                                                                 |
| enable_header_write                   | boolean | 否       | false                                      | 仅在 file_format_type 为 text,csv 时使用。<br/> false:不写入表头,true:写入表头。                                                                          |
| encoding                              | string  | 否       | "UTF-8"                                    | 仅在 file_format_type 为 json,text,csv,xml 时使用。                                                                                                                  |
| schema_save_mode                      | string  | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST               | 现有目录处理方法                                                                                                                                         |
| data_save_mode                        | string  | 否       | APPEND_DATA                                | 现有数据处理方法                                                                                                                                        |

### host [string]

需要目标 ftp 主机

### port [int]

必需传目标 ftp 端口

### user [string]

必需传目标 ftp 用户名 

### password [string]

必需传目标 ftp 密码


### path [string]

必需传目标目录路径。


### connection_mode [string]

目标 ftp 连接模式，默认为 active 模式，支持以下模式：

`active_local` `passive_local`

### custom_filename [boolean]

是否自定义文件名

### file_name_expression [string]

仅在 `custom_filename` 是 `true` 使用

`file_name_expression描述了将在`path`中创建的文件表达式。我们可以在"file_name_expression"中添加变量"${now}"或"${uuid}"，类似于"test"_${uuid}_${现在}`，
`${now}`表示当前时间，其格式可以通过指定选项`filename_time_format`来定义。

请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。

### filename_time_format [string]

仅在 `custom_filename` 是 `true` 使用

当 `file_name_expression` 参数中的格式为 `xxxx-${now}` 时，`filename_time_format` 可以指定路径的时间格式，默认值为 `yyyy.MM.dd`。常用的时间格式如下所示：

| 符号 |    描述     |
|----|-----------|
| y  | 年         |
| M  | 月         |
| d  | 日         |
| H  | 小时 (0-23) |
| m  | 分钟        |
| s  | 秒         |

### file_format_type [string]

我们支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

请注意，最终文件名将以 file_format_type 的后缀结尾，文本文件的后缀为 `txt`。

### field_delimiter [string]

数据行中列之间的分隔符。仅在 `text` 文件格式下需要。

### row_delimiter [string]

文件中行之间的分隔符。仅在 `text` 文件格式下需要。

### have_partition [boolean]

是否需要处理分区。

### partition_by [array]

仅在 `have_partition` 为 `true` 时使用。

基于选定字段进行数据分区。

### partition_dir_expression [string]

仅在 `have_partition` 为 `true` 时使用。

如果指定了 `partition_by`，我们将基于分区信息生成相应的分区目录，最终文件将放置在分区目录中。

默认的 `partition_dir_expression` 是 `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`。`k0` 是第一个分区字段，`v0` 是第一个分区字段的值。

### is_partition_field_write_in_file [boolean]

仅在 `have_partition` 为 `true` 时使用。

如果 `is_partition_field_write_in_file` 为 `true`，分区字段及其值将写入数据文件。

例如，如果你想写入一个 Hive 数据文件，其值应该为 `false`。

### sink_columns [array]

需要写入文件的列，默认值为从 `Transform` 或 `Source` 获取的所有列。字段的顺序决定了实际写入文件的顺序。

### is_enable_transaction [boolean]

如果 `is_enable_transaction` 为 true，我们将确保数据在写入目标目录时不会丢失或重复。

请注意，如果 `is_enable_transaction` 为 true，我们将自动在文件名前添加 `${transactionId}_`。

目前仅支持 `true`。

### batch_size [int]

文件中的最大行数。对于 SeaTunnel Engine，文件中的行数由 `batch_size` 和 `checkpoint.interval` 共同决定。如果 `checkpoint.interval` 的值足够大，sink writer 将在文件中的行数超过 `batch_size` 时写入文件。如果 `checkpoint.interval` 很小，当触发新检查点时，sink writer 将创建一个新文件。

### compress_codec [string]

文件的压缩编码，支持的压缩编码如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

提示：excel 类型不支持任何压缩格式

### common 选项

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

### max_rows_in_memory [int]

当文件格式为 Excel 时，内存中可以缓存的数据项最大数量。

### sheet_name [string]

工作簿的表名。

### csv_string_quote_mode [string]

当文件格式为 CSV 时，CSV 的字符串引号模式。

- ALL：所有字符串字段都会加引号。
- MINIMAL：仅为包含特殊字符（如字段分隔符、引号字符或行分隔符字符串中的任何字符）的字段加引号。
- NONE：从不为字段加引号。当数据中包含分隔符时，输出会在前面加上转义字符。如果未设置转义字符，则格式验证会抛出异常。。

### xml_root_tag [string]

指定 XML 文件中根元素的标签名。。

### xml_row_tag [string]

指定 XML 文件中数据行的标签名。

### xml_use_attr_format [boolean]

指定是否使用标签属性格式处理数据。

### parquet_avro_write_timestamp_as_int96 [boolean]

支持从时间戳写入 Parquet INT96，仅对 parquet 文件有效。

### parquet_avro_write_fixed_as_int96 [array]

支持从 12 字节字段写入 Parquet INT96，仅对 parquet 文件有效

### enable_header_write [boolean]

仅在 file_format_type 为 text,csv 时使用。false:不写入表头,true:写入表头。

### encoding [string]

仅在 file_format_type 为 json,text,csv,xml 时使用。文件写入的编码。该参数将通过 `Charset.forName(encoding)` 解析。

### schema_save_mode [string]

现有的目录处理方法。

- RECREATE_SCHEMA: 当目录不存在时创建，当目录存在时删除并重新创建
- CREATE_SCHEMA_WHEN_NOT_EXIST: 将在目录不存在时创建，在目录存在时跳过
- ERROR_WHEN_SCHEMA_NOT_EXIST: 当目录不存在时，将报告错误
- IGNORE ：忽略表的处理

### data_save_mode [string]

现有的数据处理方法：

- DROP_DATA: 保留目录并删除数据文件
- APPEND_DATA: 保存目录，保存数据文件
- ERROR_WHEN_DATA_EXISTS: 当有数据文件时，会报告错误

## 示例

用于文本文件格式的简单配置

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

用于文本文件格式 `have_partition` 和 `custom_filename` 和 `sink_columns`

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

当我们的源端是多个表，并且希望不同的表达式到不同的目录时，我们可以这样配置

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

## 更改日志

