# Hdfs文件

> Hdfs文件 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../../en/concept/connector-v2-features.md)

默认情况下，我们使用2PC提交来确保"精确一次"

- [x] 文件格式类型
  - [x] 文本
  - [x] CSV
  - [x] Parquet
  - [x] ORC
  - [x] JSON
  - [x] Excel
- [x] 压缩编解码器
  - [x] lzo

## 描述

将数据输出到Hdfs文件

## 支持的数据源信息

| 数据源    | 支持的版本            |
|--------|------------------|
| Hdfs文件 | hadoop 2.x 和 3.x |

## 接收器选项

| 名称                               | 类型      | 是否必须 | 默认值                                        | 描述                                                                                                                                                                                                                                                                                               |
|----------------------------------|---------|------|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fs.defaultFS                     | string  | 是    | -                                          | 以 `hdfs://` 开头的 Hadoop 集群地址，例如：`hdfs://hadoopcluster`                                                                                                                                                                                                                                            |
| path                             | string  | 是    | -                                          | 目标目录路径是必需的。                                                                                                                                                                                                                                                                                      |
| tmp_path                         | string  | 是    | /tmp/seatunnel                             | 结果文件将首先写入临时路径，然后使用 `mv` 命令将临时目录提交到目标目录。需要一个Hdfs路径。                                                                                                                                                                                                                                               |
| hdfs_site_path                   | string  | 否    | -                                          | `hdfs-site.xml` 的路径，用于加载 namenodes 的 ha 配置。                                                                                                                                                                                                                                                      |
| custom_filename                  | boolean | 否    | false                                      | 是否需要自定义文件名                                                                                                                                                                                                                                                                                       |
| file_name_expression             | string  | 否    | "${transactionId}"                         | 仅在 `custom_filename` 为 `true` 时使用。`file_name_expression` 描述将创建到 `path` 中的文件表达式。我们可以在 `file_name_expression` 中添加变量 `${now}` 或 `${uuid}`，例如 `test_${uuid}_${now}`，`${now}` 表示当前时间，其格式可以通过指定选项 `filename_time_format` 来定义。请注意，如果 `is_enable_transaction` 为 `true`，我们将在文件头部自动添加 `${transactionId}_`。 |
| filename_time_format             | string  | 否    | "yyyy.MM.dd"                               | 仅在 `custom_filename` 为 `true` 时使用。当 `file_name_expression` 参数中的格式为 `xxxx-${now}` 时，`filename_time_format` 可以指定路径的时间格式，默认值为 `yyyy.MM.dd`。常用的时间格式如下所示：[y:年,M:月,d:月中的一天,H:一天中的小时（0-23），m:小时中的分钟，s:分钟中的秒]                                                                                            |
| file_format_type                 | string  | 否    | "csv"                                      | 我们支持以下文件类型：`text` `json` `csv` `orc` `parquet` `excel`。请注意，最终文件名将以文件格式的后缀结束，文本文件的后缀是 `txt`。                                                                                                                                                                                                      |
| field_delimiter                  | string  | 否    | '\001'                                     | 仅在 file_format 为 text 时使用，数据行中列之间的分隔符。仅需要 `text` 文件格式。                                                                                                                                                                                                                                           |
| row_delimiter                    | string  | 否    | "\n"                                       | 仅在 file_format 为 text 时使用，文件中行之间的分隔符。仅需要 `text` 文件格式。                                                                                                                                                                                                                                            |
| have_partition                   | boolean | 否    | false                                      | 是否需要处理分区。                                                                                                                                                                                                                                                                                        |
| partition_by                     | array   | 否    | -                                          | 仅在 have_partition 为 true 时使用，根据选定的字段对数据进行分区。                                                                                                                                                                                                                                                     |
| partition_dir_expression         | string  | 否    | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 仅在 have_partition 为 true 时使用，如果指定了 `partition_by`，我们将根据分区信息生成相应的分区目录，并将最终文件放置在分区目录中。默认 `partition_dir_expression` 为 `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`。`k0` 是第一个分区字段，`v0` 是第一个分区字段的值。                                                                                                    |
| is_partition_field_write_in_file | boolean | 否    | false                                      | 仅当 `have_partition` 为 `true` 时使用。如果 `is_partition_field_write_in_file` 为 `true`，则分区字段及其值将写入数据文件中。例如，如果要写入Hive数据文件，则其值应为 `false`。                                                                                                                                                                 |
| sink_columns                     | array   | 否    |                                            | 当此参数为空时，所有字段都是接收器列。需要写入文件的列，默认值是从 `Transform` 或 `Source` 获取的所有列。字段的顺序确定了实际写入文件时的顺序。                                                                                                                                                                                                              |
| is_enable_transaction            | boolean | 否    | true                                       | 如果 `is_enable_transaction` 为 true，则在将数据写入目标目录时，我们将确保数据不会丢失或重复。请注意，如果 `is_enable_transaction` 为 `true`，我们将在文件头部自动添加 `${transactionId}_`。目前仅支持 `true`。                                                                                                                                             |
| batch_size                       | int     | 否    | 1000000                                    | 文件中的最大行数。对于 SeaTunnel Engine，文件中的行数由 `batch_size` 和 `checkpoint.interval` 共同决定。如果 `checkpoint.interval` 的值足够大，则接收器写入器将在文件中写入行，直到文件中的行大于 `batch_size`。如果 `checkpoint.interval` 很小，则接收器写入器将在新检查点触发时创建一个新文件。                                                                                        |
| single_file_mode                 | boolean | 否    | false                                      | 每个并行度只会输出一个文件，当此参数开启时，batch_size就不会生效。输出的文件名没有文件块后缀。                                                                                                                                                                                                                                             |
| create_empty_file_when_no_data   | boolean | 否    | false                                      | 当上游没有数据同步时，依然生成对应的数据文件。                                                                                                                                                                                                                                                                          |
| compress_codec                   | string  | 否    | none                                       | 文件的压缩编解码器及其支持的细节如下所示：[txt: `lzo` `none`，json: `lzo` `none`，csv: `lzo` `none`，orc: `lzo` `snappy` `lz4` `zlib` `none`，parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`]。提示：excel类型不支持任何压缩格式。                                                                                           |
| krb5_path                        | string  | 否    | /etc/krb5.conf                             | kerberos 的 krb5 路径                                                                                                                                                                                                                                                                               |
| kerberos_principal               | string  | 否    | -                                          | kerberos 的主体                                                                                                                                                                                                                                                                                     |
| kerberos_keytab_path             | string  | 否    | -                                          | kerberos 的 keytab 路径                                                                                                                                                                                                                                                                             |
| compress_codec                   | string  | 否    | none                                       | 压缩编解码器                                                                                                                                                                                                                                                                                           |
| common-options                   | object  | 否    | -                                          | 接收器插件通用参数，请参阅 [接收器通用选项](../sink-common-options.md) 了解详情                                                                                                                                                                                                                                          |
| csv_string_quote_mode            | enum    | 否    | MINIMAL                                    | 仅在文件格式为 CSV 时使用。                                                                                                                                                                                                                                                                                 |
| enable_header_write              | boolean | 否    | false                                      | 仅在 file_format_type 为 text,csv 时使用。<br/> false:不写入表头,true:写入表头。                                                                                                                                                                                                                                  |
| max_rows_in_memory               | int     | 否    | -                                          | 仅当 file_format 为 excel 时使用。当文件格式为 Excel 时，可以缓存在内存中的最大数据项数。                                                                                                                                                                                                                                       |
| sheet_name                       | string  | 否    | Sheet${Random number}                      | 仅当 file_format 为 excel 时使用。将工作簿的表写入指定的表名                                                                                                                                                                                                                                                         |
| remote_user                      | string  | 否    | -                                          | Hdfs的远端用户名。                                                                                                                                                                                                                                                                                      |

### 提示

> 如果您使用 spark/flink，为了使用此连接器，您必须确保您的 spark/flink 集群已经集成了 hadoop。测试过的 hadoop 版本是
> 2.x。如果您使用 SeaTunnel Engine，则在下载和安装 SeaTunnel Engine 时会自动集成 hadoop
> jar。您可以检查 `${SEATUNNEL_HOME}/lib` 下的 jar 包来确认这一点。

## 任务示例

### 简单示例:

> 此示例定义了一个 SeaTunnel 同步任务，通过 FakeSource 自动生成数据并将其发送到 Hdfs。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件 **仅用于测试和演示功能源插件**
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        c_map = "map<string, smallint>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
  # 如果您想获取有关如何配置 seatunnel 的更多信息和查看完整的源端插件列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # 如果您想获取有关如何配置 seatunnel 的更多信息和查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    HdfsFile {
      fs.defaultFS = "hdfs://hadoopcluster"
      path = "/tmp/hive/warehouse/test2"
      file_format_type = "orc"
    }
  # 如果您想获取有关如何配置 seatunnel 的更多信息和查看完整的接收器插件列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### orc 文件格式的简单配置

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    file_format_type = "orc"
}
```

### text 文件格式的配置，包括 `have_partition`、`custom_filename` 和 `sink_columns`

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
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

### parquet 文件格式的配置，包括 `have_partition`、`custom_filename` 和 `sink_columns`

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    file_format_type = "parquet"
    sink_columns = ["name","age"]
    is_enable_transaction = true
}
```

### enable_header_write [boolean]

仅在 file_format_type 为 text,csv 时使用。false:不写入表头,true:写入表头。

### csv_string_quote_mode [string]

当文件格式为 CSV 时，CSV 的字符串引号模式。

- ALL：所有字符串字段都会加引号。
- MINIMAL：仅为包含特殊字符（如字段分隔符、引号字符或行分隔符字符串中的任何字符）的字段加引号。
- NONE：从不为字段加引号。当数据中包含分隔符时，输出会在前面加上转义字符。如果未设置转义字符，则格式验证会抛出异常。

### kerberos 的简单配置

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    hdfs_site_path = "/path/to/your/hdfs_site_path"
    kerberos_principal = "your_principal@EXAMPLE.COM"
    kerberos_keytab_path = "/path/to/your/keytab/file.keytab"
}
```

### 压缩的简单配置

```
HdfsFile {
    fs.defaultFS = "hdfs://hadoopcluster"
    path = "/tmp/hive/warehouse/test2"
    compress_codec = "lzo"
}
```

