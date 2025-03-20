import ChangeLog from '../changelog/connector-file-obs.md';

# ObsFile

> Obs file sink 连接器

## 支持这些引擎

> Spark
>
> Flink
>
> Seatunnel Zeta

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)

默认情况下，我们使用2PC commit来确保“精确一次”`

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel

## 描述

将数据输出到华为云obs文件系统。

如果你使用spark/flink，为了使用这个连接器，你必须确保你的spark/flink集群已经集成了hadoop。测试的hadoop版本是2.x。

如果你使用SeaTunnel Engine，当你下载并安装SeaTunnel引擎时，它会自动集成hadoop jar。您可以在${SEATUNNEL_HOME}/lib下检查jar包以确认这一点。

为了支持更多的文件类型，我们进行了一些权衡，因此我们使用HDFS协议对OBS进行内部访问，而这个连接器需要一些hadoop依赖。
它只支持hadoop版本**2.9.X+**。

## 所需Jar包列表

|        jar         |     支持的版本              | Maven下载链接                                                                                         |
|--------------------|-----------------------------|---------------------------------------------------------------------------------------------------|
| hadoop-huaweicloud | support version >= 3.1.1.29 | [下载](https://repo.huaweicloud.com/artifactory/sdk_public/org/apache/hadoop/hadoop-huaweicloud/) |
| esdk-obs-java      | support version >= 3.19.7.3 | [下载](https://repo.huaweicloud.com/artifactory/sdk_public/com/huawei/storage/esdk-obs-java/)     |
| okhttp             | support version >= 3.11.0   | [下载](https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/)                               |
| okio               | support version >= 1.14.0   | [下载](https://repo1.maven.org/maven2/com/squareup/okio/okio/)                                    |

>请下载“Maven”对应的支持列表，并将其复制到“$SEATUNNEL_HOME/plugins/jdbc/lib/”工作目录。
>
>并将所有jar复制到$SEATUNNEL_HOME/lib/

## 参数 

| 名称                              | 类型    | 是否必填  | 默认值                                      | 描述                                                                                                                            |
|----------------------------------|---------|---------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| path                             | string  | 是       | -                                          | 目标目录路径。                                                                                                                    |
| bucket                           | string  | 是       | -                                          | obs文件系统的bucket地址，例如：`obs://obs-bucket-name`.                                                                            |
| access_key                       | string  | 是       | -                                          | obs文件系统的访问密钥。                                                                                                            |
| access_secret                    | string  | 是       | -                                          | obs文件系统的访问私钥。                                                                                                            |
| endpoint                         | string  | 是       | -                                          | obs文件系统的终端。                                                                                                                |
| custom_filename                  | boolean | 否       | false                                      | 是否需要自定义文件名。                                                                                                              |
| file_name_expression             | string  | 否       | "${transactionId}"                         | 描述将在“路径”中创建的文件表达式。仅在custom_filename为true时使用。[提示]（#file_name_expression）                                        |
| filename_time_format             | string  | 否       | "yyyy.MM.dd"                               | 指定“path”的时间格式。仅在custom_filename为true时使用。[提示]（#filename_time_format）                                                 |
| file_format_type                 | string  | 否       | "csv"                                      | 支持的文件类型。[提示]（#file_format_type）                                                                                          |
| field_delimiter                  | string  | 否       | '\001'                                     | 数据行中列之间的分隔符。仅在file_format为文本时使用。                                                                                   |
| row_delimiter                    | string  | 否       | "\n"                                       | 文件中行之间的分隔符。仅被“text”文件格式需要。                                                                                          |
| have_partition                   | boolean | 否       | false                                      | 是否需要处理分区。                                                                                                                  |
| partition_by                     | array   | 否       | -                                          | 根据所选字段对数据进行分区。只有在have_partition为true时才使用。                                                                         |
| partition_dir_expression         | string  | 否       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 只有在have_partition为真true时才使用。[提示]（#partition_dir_expression）                                                             |
| is_partition_field_write_in_file | boolean | 否       | false                                      | 只有在have_partition为true时才使用。[提示]（#is_partition_field_write_in_file）                                                       |
| sink_columns                     | array   | 否       |                                            | 当此参数为空时，所有字段都是接收列。[提示]（#sink_columns）                                                                              |
| is_enable_transaction            | boolean | 否       | true                                       | [提示](#is_enable_transaction)                                                                                                    |
| batch_size                       | int     | 否       | 1000000                                    | [提示](#batch_size)                                                                                                               |
| single_file_mode                 | boolean | 否       | false                                      | 每个并行处理只会输出一个文件。启用此参数后，batch_size将不会生效。输出文件名没有文件块后缀。                                                   |
| create_empty_file_when_no_data   | boolean | 否       | false                                      | 当上游没有数据同步时，仍然会生成相应的数据文件。                                                                                         |
| compress_codec                   | string  | 否       | none                                       | [提示](#compress_codec)                                                                                                           |
| common-options                   | object  | 否       | -                                          | [提示](#common_options)                                                                                                           |
| max_rows_in_memory               | int     | 否       | -                                          | 当文件格式为Excel时，内存中可以缓存的最大数据项数。仅在file_format为excel时使用。                                                           |
| sheet_name                       | string  | 否       | Sheet${Random number}                      | 标签页。仅在file_format为excel时使用。                                                                                                |

### 提示

#### <span id="file_name_expression"> file_name_expression </span>

>仅在“custom_filename”为“true”时使用。
>
>`file_name_expression`描述了将在`path`中创建的文件表达式。
>
>我们可以在“file_name_expression”中添加变量“${now}”或“${uuid}”，类似于“test_${uuid}_${now}”，
>
>“${now}”表示当前时间，其格式可以通过指定选项“filename_time_format”来定义。
请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。

#### <span id="filename_time_format"> filename_time_format </span>

>仅在“custom_filename”为“true”时使用。
>
>当`file_name_expression`参数中的格式为`xxxx-${now}`时，`filename_time_format`可以指定路径的时间格式，默认值为`yyyy.MM.dd`。常用的时间格式如下：

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

#### <span id="file_format_type"> file_format_type </span>

>我们支持以下文件类型：
>
> `text` `json` `csv` `orc` `parquet` `excel`

请注意，最终文件名将以file_format的后缀结尾，文本文件的后缀为“txt”。

#### <span id="partition_dir_expression"> partition_dir_expression </span>

>仅在“have_partition”为“true”时使用。
>
>如果指定了`partition_by`，我们将根据分区信息生成相应的分区目录，并将最终文件放置在分区目录中。
>
>默认的`partition_dir_expression`是`${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`.`k0`是第一个分区字段，`v0`是第一个划分字段的值。

#### <span id="is_partition_field_write_in_file"> is_partition_field_write_in_file </span>

>仅在“have_partition”为“true”时使用。
>
>如果`is_partition_field_write_in_file`为`true`，则分区字段及其值将写入数据文件。
>
>例如，如果你想写一个Hive数据文件，它的值应该是“false”。

#### <span id="sink_columns"> sink_columns </span>

>哪些列需要写入文件，默认值是从“Transform”或“Source”获取的所有列。
>字段的顺序决定了文件实际写入的顺序。

#### <span id="is_enable_transaction"> is_enable_transaction </span>

>如果`is_enable_transaction`为`true`，我们将确保数据在写入目标目录时不会丢失或重复。
>
>请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。现在只支持“true”。

#### <span id="batch_size"> batch_size </span>

>文件中的最大行数。对于SeaTunnel引擎，文件中的行数由“batch_size”和“checkpoint.interval”共同决定。如果“checkpoint.interval”的值足够大，sink writer将在文件中写入行，直到文件中的行大于“batch_size”。如果“checkpoint.interval”较小，则接收器写入程序将在新的检查点触发时创建一个新文件。

#### <span id="compress_codec"> compress_codec </span>

>文件的压缩编解码器和支持的详细信息如下所示：
>
> - txt: `lzo` `none`
> - json: `lzo` `none`
> - csv: `lzo` `none`
> - orc: `lzo` `snappy` `lz4` `zlib` `none`
> - parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

请注意，excel类型不支持任何压缩格式

#### <span id="common_options"> common options </span>

>Sink插件常用参数，请参考[Sink common Options]（../Sink-common-Options.md）了解详细信息。

## 任务示例

### text 文件

>对于具有“have_partition”、“custom_filename”和“sink_columns”的文本文件格式。

```hocon

  ObsFile {
    path="/seatunnel/text"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
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

### parquet 文件

>适用于带有“have_partition”和“sink_columns”的拼花地板文件格式。

```hocon

  ObsFile {
    path = "/seatunnel/parquet"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }

```

### orc 文件

>对于orc文件格式的简单配置。

```hocon

  ObsFile {
    path="/seatunnel/orc"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "obs.xxxxx.myhuaweicloud.com"
    file_format_type = "orc"
  }

```

### json 文件

>对于json文件格式简单配置。

```hcocn

   ObsFile {
       path = "/seatunnel/json"
       bucket = "obs://obs-bucket-name"
       access_key = "xxxxxxxxxxx"
       access_secret = "xxxxxxxxxxx"
       endpoint = "obs.xxxxx.myhuaweicloud.com"
       file_format_type = "json"
   }

```

### excel 文件

>对于excel文件格式简单配置。

```hcocn

   ObsFile {
       path = "/seatunnel/excel"
       bucket = "obs://obs-bucket-name"
       access_key = "xxxxxxxxxxx"
       access_secret = "xxxxxxxxxxx"
       endpoint = "obs.xxxxx.myhuaweicloud.com"
       file_format_type = "excel"
   }

```

### csv 文件

>对于csv文件格式简单配置。

```hcocn

   ObsFile {
       path = "/seatunnel/csv"
       bucket = "obs://obs-bucket-name"
       access_key = "xxxxxxxxxxx"
       access_secret = "xxxxxxxxxxx"
       endpoint = "obs.xxxxx.myhuaweicloud.com"
       file_format_type = "csv"
   }

```

## 变更日志

<ChangeLog />