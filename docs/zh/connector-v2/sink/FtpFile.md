# FtpFile

> Ftp文件接收器连接器

## 描述

将数据输出到Ftp。

:::提示

如果你使用spark/flink，为了使用这个连接器，你必须确保你的spark/flilk集群已经集成了hadoop。测试的hadoop版本是2.x。

如果你使用SeaTunnel Engine，当你下载并安装SeaTunnel引擎时，它会自动集成hadoop jar。您可以在${SEATUNNEL_HOME}/lib下检查jar包以确认这一点。

:::

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)

默认情况下，我们使用2PC commit来确保 `exactly-once`

- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## 选项

| Name                                  | Type    | Required | Default                                    | Description                                                                                                                                                            |
|---------------------------------------|---------|----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | string  | yes      | -                                          |                                                                                                                                                                        |
| port                                  | int     | yes      | -                                          |                                                                                                                                                                        |
| user                                  | string  | yes      | -                                          |                                                                                                                                                                        |
| password                              | string  | yes      | -                                          |                                                                                                                                                                        |
| path                                  | string  | yes      | -                                          |                                                                                                                                                                        |
| tmp_path                              | string  | yes      | /tmp/seatunnel                             | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir. Need a FTP dir.                                                      |
| connection_mode                       | string  | no       | active_local                               | The target ftp connection mode                                                                                                                                         |
| custom_filename                       | boolean | no       | false                                      | Whether you need custom the filename                                                                                                                                   |
| file_name_expression                  | string  | no       | "${transactionId}"                         | Only used when custom_filename is true                                                                                                                                 |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                               | Only used when custom_filename is true                                                                                                                                 |
| file_format_type                      | string  | no       | "csv"                                      |                                                                                                                                                                        |
| field_delimiter                       | string  | no       | '\001'                                     | Only used when file_format_type is text                                                                                                                                |
| row_delimiter                         | string  | no       | "\n"                                       | Only used when file_format_type is text                                                                                                                                |
| have_partition                        | boolean | no       | false                                      | Whether you need processing partitions.                                                                                                                                |
| partition_by                          | array   | no       | -                                          | Only used then have_partition is true                                                                                                                                  |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used then have_partition is true                                                                                                                                  |
| is_partition_field_write_in_file      | boolean | no       | false                                      | Only used then have_partition is true                                                                                                                                  |
| sink_columns                          | array   | no       |                                            | When this parameter is empty, all fields are sink columns                                                                                                              |
| is_enable_transaction                 | boolean | no       | true                                       |                                                                                                                                                                        |
| batch_size                            | int     | no       | 1000000                                    |                                                                                                                                                                        |
| compress_codec                        | string  | no       | none                                       |                                                                                                                                                                        |
| common-options                        | object  | no       | -                                          |                                                                                                                                                                        |
| max_rows_in_memory                    | int     | no       | -                                          | Only used when file_format_type is excel.                                                                                                                              |
| sheet_name                            | string  | no       | Sheet${Random number}                      | Only used when file_format_type is excel.                                                                                                                              |
| csv_string_quote_mode                 | enum    | no       | MINIMAL                                    | Only used when file_format is csv.                                                                                                                                     |
| xml_root_tag                          | string  | no       | RECORDS                                    | Only used when file_format is xml.                                                                                                                                     |
| xml_row_tag                           | string  | no       | RECORD                                     | Only used when file_format is xml.                                                                                                                                     |
| xml_use_attr_format                   | boolean | no       | -                                          | Only used when file_format is xml.                                                                                                                                     |
| single_file_mode                      | boolean | no       | false                                      | Each parallelism will only output one file. When this parameter is turned on, batch_size will not take effect. The output file name does not have a file block suffix. |
| create_empty_file_when_no_data        | boolean | no       | false                                      | When there is no data synchronization upstream, the corresponding data files are still generated.                                                                      |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                      | Only used when file_format is parquet.                                                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                          | Only used when file_format is parquet.                                                                                                                                 |
| enable_header_write                   | boolean | no       | false                                      | Only used when file_format_type is text,csv.<br/> false:don't write header,true:write header.                                                                          |
| encoding                              | string  | no       | "UTF-8"                                    | Only used when file_format_type is json,text,csv,xml.                                                                                                                  |
| schema_save_mode                      | string  | no       | CREATE_SCHEMA_WHEN_NOT_EXIST               | Existing dir processing method                                                                                                                                         |
| data_save_mode                        | string  | no       | APPEND_DATA                                | Existing data processing method                                                                                                                                        |

### host [string]

需要目标ftp主机

### port [int]

目标ftp端口是必需的

### user [string]

目标ftp用户名是必需的

### password [string]

需要目标ftp密码


### path [string]

目标目录路径是必需的。


### connection_mode [string]

目标ftp连接模式，默认为活动模式，支持以下模式：

`active_local` `passive_local`

### custom_filename [boolean]

是否自定义文件名

### file_name_expression [string]

仅在以下情况下使用 `custom_filename` 是 `true`

`file_name_expression描述了将在`path`中创建的文件表达式。我们可以在“file_name_expression”中添加变量“${now}”或“${uuid}”，类似于“test”_${uuid}_${现在}`，
`${now}`表示当前时间，其格式可以通过指定选项`filename_time_format`来定义。

请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。

### filename_time_format [string]

仅在以下情况下使用 `custom_filename` is `true`

当`file_name_expression`参数中的格式为`xxxx-${now}时，`filename_time_format`可以指定路径的时间格式，默认值为`yyyy。MM.dd。常用的时间格式如下：

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

### file_format_type [string]

我们支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

请注意，最终文件名将以file_format_type的后缀结尾，文本文件的后缀为“txt”。

### field_delimiter [string]

数据行中列之间的分隔符。只需要“文本”文件格式。

### row_delimiter [string]

文件中行之间的分隔符。只需要“文本”文件格式。

### have_partition [boolean]

是否需要处理分区。

### partition_by [array]

仅在以下情况下使用 `have_partition` is `true`.

根据所选字段对数据进行分区。

### partition_dir_expression [string]

仅在以下情况下使用 `have_partition` is `true`.

如果指定了`partition_by`，我们将根据分区信息生成相应的分区目录，并将最终文件放置在分区目录中。

默认的`partition_dir_expression`是`${k0}=${v0}/${k1}=${1v1}//${kn}=${vn}/``k0是第一个分区字段，v0是第一个划分字段的值。

### is_partition_field_write_in_file [boolean]

仅在以下情况下使用 `have_partition` is `true`.

如果`is_partition_field_write_in_file`为`true`，则分区字段及其值将写入数据文件。

例如，如果你想写一个Hive数据文件，它的值应该是“false”。

### sink_columns [array]

哪些列需要写入文件，默认值是从“Transform”或“Source”获取的所有列。
字段的顺序决定了文件实际写入的顺序。

### is_enable_transaction [boolean]

如果`is_enable_transaction`为true，我们将确保数据在写入目标目录时不会丢失或重复。

请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。

现在只支持“true”。

### batch_size [int]

文件中的最大行数。对于SeaTunnel引擎，文件中的行数由“batch_size”和“checkpoint.interval”共同决定。如果“checkpoint.interval”的值足够大，sink writer将在文件中写入行，直到文件中的行大于“batch_size”。如果“checkpoint.interval”较小，则接收器写入程序将在新的检查点触发时创建一个新文件。

### compress_codec [string]

文件的压缩编解码器和支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

提示：excel类型不支持任何压缩格式

### common 选项

Sink插件常用参数，请参考[Sink common Options]（../sink-common-options.md）了解详细信息。

### max_rows_in_memory [int]

当文件格式为Excel时，内存中可以缓存的最大数据项数。

### sheet_name [string]

编写工作簿的工作表

### csv_string_quote_mode [string]

当文件格式为CSV时，CSV的字符串引用模式。

- ALL: 所有字符串字段都将被引用。
- MINIMAL: 引号字段包含特殊字符，如字段分隔符、引号字符或行分隔符字符串中的任何字符。
- NONE:从不引用字段。当分隔符出现在数据中时，打印机会用转义符作为前缀。如果未设置转义符，格式验证将抛出异常。

### xml_root_tag [string]

指定XML文件中根元素的标记名。

### xml_row_tag [string]

指定XML文件中数据行的标记名称。

### xml_use_attr_format [boolean]

指定是否使用标记属性格式处理数据。

### parquet_avro_write_timestamp_as_int96 [boolean]

支持从时间戳写入Parquet INT96，仅适用于拼花地板文件。

### parquet_avro_write_fixed_as_int96 [array]

支持从12字节字段写入Parquet INT96，仅适用于拼花地板文件。

### enable_header_write [boolean]

仅在以下情况下使用 file_format_type是文本，csv。false：不写标头，true：写标头。

### encoding [string]

仅在以下情况下使用 file_format_type是json、文本、csv、xml。
要写入的文件的编码。此参数将由解析 `Charset.forName(encoding)`.

### schema_save_mode [string]

现有的目录处理方法。

- RECREATE_SCHEMA: 当目录不存在时创建，当目录存在时删除并重新创建
- CREATE_SCHEMA_WHEN_NOT_EXIST: 将在目录不存在时创建，在目录存在时跳过
- ERROR_WHEN_SCHEMA_NOT_EXIST: 当目录不存在时，将报告错误
- IGNORE ：忽略桌子的处理

### data_save_mode [string]

现有的数据处理方法。

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

## 修改日志

### 2.2.0-beta 2022-09-26

- 添加Ftp文件接收器连接器

### 2.3.0-beta 2022-10-20

- [BugFix] 修复windows环境下路径错误的bug ([2980](https://github.com/apache/seatunnel/pull/2980))
- [BugFix] 修复文件系统获取错误 ([3117](https://github.com/apache/seatunnel/pull/3117))
- [BugFix] 解决了无法从配置文件中将“\t”解析为分隔符的错误 ([3083](https://github.com/apache/seatunnel/pull/3083))

### 下一版本

- [BugFix] 修复了以下无法将数据写入文件的错误 ([3258](https://github.com/apache/seatunnel/pull/3258))
  - 当上游的字段为空时，它将抛出NullPointerException
  - 接收器列映射失败
  - 当从状态还原写入程序时，直接获取事务失败
- [Improve] 支持为每个文件设置批量大小 ([3625](https://github.com/apache/seatunnel/pull/3625))
- [Improve] 支持文件压缩 ([3899](https://github.com/apache/seatunnel/pull/3899))

