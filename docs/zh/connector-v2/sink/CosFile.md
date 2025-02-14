# CosFile

> Cos 文件接收器连接器

## 描述

将数据输出到cos文件系统.

:::提示

如果你使用spark/flink，为了使用这个连接器，你必须确保你的spark/flilk集群已经集成了hadoop。测试的hadoop版本是2.x

如果你使用SeaTunnel Engine，当你下载并安装SeaTunnel引擎时，它会自动集成hadoop jar。您可以在${SEATUNNEL_HOME}/lib下检查jar包以确认这一点.

要使用此连接器，您需要将hadoop cos-{hadoop.version}-{version}.jar和cos_api-bundle-{version}.jar位于${SEATUNNEL_HOME}/lib目录中，下载：[Hoop cos发布](https://github.com/tencentyun/hadoop-cos/releases). 它只支持hadoop 2.6.5+和8.0.2版本+.

:::

## 关键特性

- [x] [精确一次](../../concept/connector-v2-features.md)

默认情况下，我们使用2PC commit来确保 `精确一次`

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## 选项

| 名称           |  类型  | 必需 | 默认值                                    | 描述                                                                                                                                                                    |
|---------------------------------------|---------|----|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | 是  | -                                          |                                                                                                                                                                       |
| tmp_path                              | string  | 否  | /tmp/seatunnel                             | 结果文件将首先写入tmp路径，然后使用“mv”将tmp目录提交到目标目录。需要一个COS目录.                                                                                                                       |
| bucket                                | string  | 是  | -                                          |                                                                                                                                                                       |
| secret_id                             | string  | 是  | -                                          |                                                                                                                                                                       |
| secret_key                            | string  | 是  | -                                          |                                                                                                                                                                       |
| region                                | string  | 是  | -                                          |                                                                                                                                                                       |
| custom_filename                       | boolean | 否 | false                                      | 是否需要自定义文件名                                                                                                                                                            |
| file_name_expression                  | string  | 否 | "${transactionId}"                         | 仅在custom_filename为true时使用                                                                                                                                             |
| filename_time_format                  | string  | 否 | "yyyy.MM.dd"                               | 仅在custom_filename为true时使用                                                                                                                                             |
| file_format_type                      | string  | 否 | "csv"                                      |                                                                                                                                                                       |
| field_delimiter                       | string  | 否 | '\001'                                     | 仅在file_format为text时使用                                                                                                                                                 |
| row_delimiter                         | string  | 否 | "\n"                                       | 仅在file_format为text时使用                                                                                                                                                   |
| have_partition                        | boolean | 否 | false                                      | 是否需要处理分区.                                                                                                                                                             |
| partition_by                          | array   | 否 | -                                          | 只有在have_partition为true时才使用                                                                                                                                            |
| partition_dir_expression              | string  | 否 | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 只有在have_partition为true时才使用                                                                                                                                  |
| is_partition_field_write_in_file      | boolean | 否 | false                                      | 只有在have_partition为true时才使用                                                                                                                                 |
| sink_columns                          | array   | 否 |                                            | 当此参数为空时，所有字段都是接收列                                                                                                           |
| is_enable_transaction                 | boolean | 否 | true                                       |                                                                                                                                                                       |
| batch_size                            | int     | 否 | 1000000                                    |                                                                                                                                                                       |
| compress_codec                        | string  | 否 | none                                       |                                                                                                                                                                       |
| common-options                        | object  | 否 | -                                          |                                                                                                                                                                       |
| max_rows_in_memory                    | int     | 否 | -                                          | 仅在file_format为excel时使用.                                                                                                                                  |
| sheet_name                            | string  | 否 | Sheet${Random number}                      | 仅在file_format为excel时使用.                                                                                                                                  |
| csv_string_quote_mode                 | enum    | 否 | MINIMAL                                    | 仅在file_format为csv时使用.                                                                                                                                    |
| xml_root_tag                          | string  | 否 | RECORDS                                    | 仅在file_format为xml时使用.                                                                                                                                    |
| xml_row_tag                           | string  | 否 | RECORD                                     | 仅在file_format为xml时使用.                                                                                                                                    |
| xml_use_attr_format                   | boolean | 否 | -                                          | 仅在file_format为xml时使用.                                                                                                                                    |
| single_file_mode                      | boolean | 否 | false                                      | 每个并行处理只会输出一个文件。启用此参数后，batch_size将不会生效。输出文件名没有文件块后缀. |
| create_empty_file_when_no_data        | boolean | 否 | false                                      | 当上游没有数据同步时，仍然会生成相应的数据文件.                                                                     |
| parquet_avro_write_timestamp_as_int96 | boolean | 否 | false                                      | 仅在file_format为parquet时使用.                                                                                                                                |
| parquet_avro_write_fixed_as_int96     | array   | 否 | -                                          | 仅在file_format为parquet时使用.                                                                                                                                |
| encoding                              | string  | 否 | "UTF-8"                                    | 仅当file_format_type为json、text、csv、xml时使用.                                                                                                                 |

### path [string]

目标目录路径是必需的.

### bucket [string]

cos文件系统的bucket地址，例如：`cosn://seatunnel-test-1259587829`

### secret_id [string]

cos文件系统的密钥id.

### secret_key [string]

cos文件系统的密钥.

### region [string]

cos文件系统的分区.

### custom_filename [boolean]

是否自定义文件名

### file_name_expression [string]

仅在 `custom_filename` 为 `true`时使用

`file_name_expression`描述了将在`path`中创建的文件表达式。我们可以在`file_name_expression`中添加变量`${now}`或`${uuid}`，类似于`test_${uuid}_${now}`，
`${now}`表示当前时间，其格式可以通过指定选项`filename_time_format`来定义.

请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头

### filename_time_format [string]

仅在 `custom_filename` 为 `true` 时使用`

当 `file_name_expression` 参数中的格式为 `xxxx-${now}` 时，`filename_time_format` 可以指定路径的时间格式，默认值为 `yyyy.MM.dd`。常用的时间格式如下：

| 符号| 描述       |
|--------|----------|
| y      | 年        |
| M      | 月        |
| d      | 日        |
| H      | 时 (0-23) |
| m      | 分        |
| s      | 秒        |

### file_format_type [string]

我们支持以下文件类型:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

请注意，最终文件名将以 file_format 的后缀结尾, 文本文件的后缀为 `txt`.

### field_delimiter [string]

数据行中列之间的分隔符. 仅需要 `text` 文件格式.

### row_delimiter [string]

文件中行之间的分隔符. 只需要 `text` 文件格式.

### have_partition [boolean]

是否需要处理分区.

### partition_by [array]

仅在 `have_partition` 为 `true` 时使用.

基于选定字段对数据进行分区.

### partition_dir_expression [string]

仅在 `have_partition` 为 `true` 时使用.

如果指定了 `partition_by` ，我们将根据分区信息生成相应的分区目录，并将最终文件放置在分区目录中。
默认的 `partition_dir_expression` 是 `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` 是第一个分区字段 , `v0` 是第一个划分字段的值.

### is_partition_field_write_in_file [boolean]

仅在 `have_partition` 为 `true` 时使用.

如果 `is_partition_field_write_in_file` 为 `true`, 分区字段及其值将写入数据文件.

例如，如果你想写一个Hive数据文件，它的值应该是 `false`.

### sink_columns [array]

哪些列需要写入文件，默认值是从 `Transform` 或 `Source` 获取的所有列.
字段的顺序决定了文件实际写入的顺序.

### is_enable_transaction [boolean]

如果 `is_enable_transaction` 为 `true`, 我们将确保数据在写入目标目录时不会丢失或重复.

请注意，如果 `is_enable_transaction` 为 `true`, 我们将自动添加 `${transactionId}_` 在文件的开头.

现在只支持 `true` .

### batch_size [int]

文件中的最大行数。对于SeaTunnel引擎，文件中的行数由 `batch_size` 和 `checkpoint.interval` 共同决定. 如果 `checkpoint.interval` 的值足够大, 接收器写入程序将在文件中写入行，直到文件中的行大于 `batch_size`. 如果 `checkpoint.interval` 较小, 则接收器写入程序将在新的检查点触发时创建一个新文件.

### compress_codec [string]

文件的压缩编解码器和支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

Tips: excel 类型不支持任何压缩格式

### common options

接收器写入插件常用参数，请参考 [Sink Common Options](../sink-common-options.md) 了解详细信息.

### max_rows_in_memory [int]

当文件格式为Excel时，内存中可以缓存的最大数据项数.

### sheet_name [string]

编写工作簿的工作表

### csv_string_quote_mode [string]

当文件格式为CSV时，CSV的字符串引用模式.

- ALL: 所有字符串字段都将被引用.
- MINIMAL: 引号字段包含特殊字符，如字段分隔符、引号字符或行分隔符字符串中的任何字符.
- NONE: 从不引用字段。当分隔符出现在数据中时，打印机会用转义符作为前缀。如果未设置转义符，格式验证将抛出异常.

### xml_root_tag [string]

指定XML文件中根元素的标记名.

### xml_row_tag [string]

指定XML文件中数据行的标记名称.

### xml_use_attr_format [boolean]

指定是否使用标记属性格式处理数据.

### parquet_avro_write_timestamp_as_int96 [boolean]

支持从时间戳写入Parquet INT96，仅适用于拼花地板文件.

### parquet_avro_write_fixed_as_int96 [array]

支持从12字节字段写入Parquet INT96，仅适用于拼花地板文件.

### encoding [string]

仅当file_format_type为json、text、csv、xml时使用.
要写入的文件的编码。此参数将由`Charset.forName(encoding)` 解析.

## 示例

对于具有 `have_partition` 、 `custom_filename` 和 `sink_columns` 的文本文件格式

```hocon

  CosFile {
    path="/sink"
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
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

```

适用于带有`have_partition` 和 `sink_columns`的parquet 文件格式`

```hocon

  CosFile {
    path="/sink"
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }

```

对于orc文件格式的简单配置

```bash

  CosFile {
    path="/sink"
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    file_format_type = "orc"
  }

```

## 变更日志

### 下一个版本

- 添加文件cos接收器连接器 ([4979](https://github.com/apache/seatunnel/pull/4979))

