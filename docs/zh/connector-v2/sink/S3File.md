# S3File

> S3 文件 Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)
- [x] [支持多表写入](../../concept/connector-v2-features.md)

默认情况下，我们使用 2PC 提交来确保 `精确一次`。

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## 描述

将数据输出到 AWS S3 文件系统。

## 支持的数据源信息

| 数据源 | 支持的版本 |
|--------|------------|
| S3     | 当前版本   |

## 数据库依赖

> 如果您使用 Spark/Flink，为了使用此连接器，您必须确保您的 Spark/Flink 集群已经集成了 Hadoop。测试的 Hadoop 版本为 2.x。
>
> 如果您使用 SeaTunnel引擎，当您下载并安装 SeaTunnel引擎时，它会自动集成 Hadoop jar 包。您可以在 `${SEATUNNEL_HOME}/lib` 下检查 jar 包以确认这一点。
> 要使用此连接器，您需要将 `hadoop-aws-3.1.4.jar` 和 `aws-java-sdk-bundle-1.12.692.jar` 放在 `${SEATUNNEL_HOME}/lib` 目录下。

## 数据类型映射

如果写入 `csv`、`text` 文件类型，所有列都将为字符串类型。

### Orc 文件类型

| SeaTunnel 数据类型 | Orc 数据类型         |
|--------------------|---------------------|
| STRING             | STRING              |
| BOOLEAN            | BOOLEAN             |
| TINYINT            | BYTE                |
| SMALLINT           | SHORT               |
| INT                | INT                 |
| BIGINT             | LONG                |
| FLOAT              | FLOAT               |
| FLOAT              | FLOAT               |
| DOUBLE             | DOUBLE              |
| DECIMAL            | DECIMAL             |
| BYTES              | BINARY              |
| DATE               | DATE                |
| TIME <br/> TIMESTAMP | TIMESTAMP           |
| ROW                | STRUCT              |
| NULL               | 不支持的数据类型     |
| ARRAY              | LIST                |
| Map                | Map                 |

### Parquet 文件类型

| SeaTunnel 数据类型 | Parquet 数据类型     |
|--------------------|---------------------|
| STRING             | STRING              |
| BOOLEAN            | BOOLEAN             |
| TINYINT            | INT_8               |
| SMALLINT           | INT_16              |
| INT                | INT32               |
| BIGINT             | INT64               |
| FLOAT              | FLOAT               |
| FLOAT              | FLOAT               |
| DOUBLE             | DOUBLE              |
| DECIMAL            | DECIMAL             |
| BYTES              | BINARY              |
| DATE               | DATE                |
| TIME <br/> TIMESTAMP | TIMESTAMP_MILLIS    |
| ROW                | GroupType           |
| NULL               | 不支持的数据类型     |
| ARRAY              | LIST                |
| Map                | Map                 |

## Sink 选项

| 名称                                  | 类型    | 是否必填 | 默认值                                               | 描述                                                                                                                                                            |
|---------------------------------------|---------|----------|-------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | 是       | -                                                     |                                                                                                                                                                |
| tmp_path                              | string  | 否       | /tmp/seatunnel                                        | 结果文件将首先写入临时路径，然后使用 `mv` 将临时目录提交到目标目录。需要一个 S3 目录。                                                                           |
| bucket                                | string  | 是       | -                                                     |                                                                                                                                                                |
| fs.s3a.endpoint                       | string  | 是       | -                                                     |                                                                                                                                                                |
| fs.s3a.aws.credentials.provider       | string  | 是       | com.amazonaws.auth.InstanceProfileCredentialsProvider | 认证 s3a 的方式。目前仅支持 `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` 和 `com.amazonaws.auth.InstanceProfileCredentialsProvider`。                  |
| access_key                            | string  | 否       | -                                                     | 仅当 fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider 时使用                                                             |
| access_secret                         | string  | 否       | -                                                     | 仅当 fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider 时使用                                                             |
| custom_filename                       | boolean | 否       | false                                                 | 是否需要自定义文件名                                                                                                                                           |
| file_name_expression                  | string  | 否       | "${transactionId}"                                    | 仅当 custom_filename 为 true 时使用                                                                                                                            |
| filename_time_format                  | string  | 否       | "yyyy.MM.dd"                                          | 仅当 custom_filename 为 true 时使用                                                                                                                            |
| file_format_type                      | string  | 否       | "csv"                                                 |                                                                                                                                                                |
| field_delimiter                       | string  | 否       | '\001'                                                | 仅当 file_format 为 text 时使用                                                                                                                                |
| row_delimiter                         | string  | 否       | "\n"                                                  | 仅当 file_format 为 text 时使用                                                                                                                                |
| have_partition                        | boolean | 否       | false                                                 | 是否需要处理分区。                                                                                                                                             |
| partition_by                          | array   | 否       | -                                                     | 仅当 have_partition 为 true 时使用                                                                                                                             |
| partition_dir_expression              | string  | 否       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"            | 仅当 have_partition 为 true 时使用                                                                                                                             |
| is_partition_field_write_in_file      | boolean | 否       | false                                                 | 仅当 have_partition 为 true 时使用                                                                                                                             |
| sink_columns                          | array   | 否       |                                                       | 当此参数为空时，所有字段均为 sink 列                                                                                                                           |
| is_enable_transaction                 | boolean | 否       | true                                                  |                                                                                                                                                                |
| batch_size                            | int     | 否       | 1000000                                               |                                                                                                                                                                |
| compress_codec                        | string  | 否       | none                                                  |                                                                                                                                                                |
| common-options                        | object  | 否       | -                                                     |                                                                                                                                                                |
| max_rows_in_memory                    | int     | 否       | -                                                     | 仅当 file_format 为 excel 时使用                                                                                                                               |
| sheet_name                            | string  | 否       | Sheet${Random number}                                 | 仅当 file_format 为 excel 时使用                                                                                                                               |
| csv_string_quote_mode                 | enum    | 否       | MINIMAL                                               | 仅当 file_format 为 csv 时使用                                                                                                                                 |
| xml_root_tag                          | string  | 否       | RECORDS                                               | 仅当 file_format 为 xml 时使用，指定 XML 文件中根元素的标签名称。                                                                                               |
| xml_row_tag                           | string  | 否       | RECORD                                                | 仅当 file_format 为 xml 时使用，指定 XML 文件中数据行的标签名称。                                                                                               |
| xml_use_attr_format                   | boolean | 否       | -                                                     | 仅当 file_format 为 xml 时使用，指定是否使用标签属性格式处理数据。                                                                                              |
| single_file_mode                      | boolean | 否       | false                                                 | 每个并行度只会输出一个文件。当此参数开启时，batch_size 将不会生效。输出文件名不会有文件块后缀。                                                                 |
| create_empty_file_when_no_data        | boolean | 否       | false                                                 | 当上游没有数据同步时，仍然会生成相应的数据文件。                                                                                                               |
| parquet_avro_write_timestamp_as_int96 | boolean | 否       | false                                                 | 仅当 file_format 为 parquet 时使用                                                                                                                             |
| parquet_avro_write_fixed_as_int96     | array   | 否       | -                                                     | 仅当 file_format 为 parquet 时使用                                                                                                                             |
| hadoop_s3_properties                  | map     | 否       |                                                       | 如果您需要添加其他选项，可以在此处添加，并参考此[链接](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)                            |
| schema_save_mode                      | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST                          | 在开启同步任务之前，对目标路径进行不同的处理                                                                                                                   |
| data_save_mode                        | Enum    | 否       | APPEND_DATA                                           | 在开启同步任务之前，对目标路径中的数据文件进行不同的处理                                                                                                       |
| enable_header_write                   | boolean | 否       | false                                                 | 仅当 file_format_type 为 text,csv 时使用。<br/> false: 不写入表头, true: 写入表头。                                                                             |
| encoding                              | string  | 否       | "UTF-8"                                               | 仅当 file_format_type 为 json,text,csv,xml 时使用。                                                                                                             |

### path [string]

存储数据文件的路径，支持变量替换。例如：path=/test/${database_name}/${schema_name}/${table_name}

### hadoop_s3_properties [map]

如果您需要添加其他选项，可以在此处添加，并参考此[链接](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

```
hadoop_s3_properties {
      "fs.s3a.buffer.dir" = "/data/st_test/s3a"
      "fs.s3a.fast.upload.buffer" = "disk"
   }
```

### custom_filename [boolean]

是否自定义文件名

### file_name_expression [string]

仅当 `custom_filename` 为 `true` 时使用

`file_name_expression` 描述了将创建到 `path` 中的文件表达式。我们可以在 `file_name_expression` 中添加变量 `${now}` 或 `${uuid}`，例如 `test_${uuid}_${now}`，
`${now}` 表示当前时间，其格式可以通过指定选项 `filename_time_format` 来定义。

请注意，如果 `is_enable_transaction` 为 `true`，我们会在文件头部自动添加 `${transactionId}_`。

### filename_time_format [string]

仅当 `custom_filename` 为 `true` 时使用

当 `file_name_expression` 参数中的格式为 `xxxx-${now}` 时，`filename_time_format` 可以指定路径的时间格式，默认值为 `yyyy.MM.dd`。常用的时间格式如下：

| 符号 | 描述               |
|------|--------------------|
| y    | 年                 |
| M    | 月                 |
| d    | 日                 |
| H    | 小时 (0-23)        |
| m    | 分钟               |
| s    | 秒                 |

### file_format_type [string]

我们支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

请注意，最终文件名将以文件格式类型的后缀结尾，文本文件的后缀为 `txt`。

### field_delimiter [string]

行数据中列之间的分隔符。仅在 `text` 文件格式中需要。

### row_delimiter [string]

文件中行之间的分隔符。仅在 `text` 文件格式中需要。

### have_partition [boolean]

是否需要处理分区。

### partition_by [array]

仅当 `have_partition` 为 `true` 时使用。

根据选定的字段对数据进行分区。

### partition_dir_expression [string]

仅当 `have_partition` 为 `true` 时使用。

如果指定了 `partition_by`，我们将根据分区信息生成相应的分区目录，最终文件将放置在分区目录中。

默认的 `partition_dir_expression` 为 `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`。`k0` 是第一个分区字段，`v0` 是第一个分区字段的值。

### is_partition_field_write_in_file [boolean]

仅当 `have_partition` 为 `true` 时使用。

如果 `is_partition_field_write_in_file` 为 `true`，分区字段及其值将被写入数据文件。

例如，如果您想写入 Hive 数据文件，其值应为 `false`。

### sink_columns [array]

哪些列需要写入文件，默认值为从 `Transform` 或 `Source` 获取的所有列。
字段的顺序决定了文件实际写入的顺序。

### is_enable_transaction [boolean]

如果 `is_enable_transaction` 为 true，我们将确保在将数据写入目标目录时不会丢失或重复。

请注意，如果 `is_enable_transaction` 为 `true`，我们会在文件头部自动添加 `${transactionId}_`。

目前仅支持 `true`。

### batch_size [int]

文件中的最大行数。对于 SeaTunnel Engine，文件中的行数由 `batch_size` 和 `checkpoint.interval` 共同决定。如果 `checkpoint.interval` 的值足够大，sink writer 将一直写入文件，直到文件中的行数超过 `batch_size`。如果 `checkpoint.interval` 较小，sink writer 将在新的 checkpoint 触发时创建一个新文件。

### compress_codec [string]

文件的压缩编解码器，支持的详细信息如下：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

提示：excel 类型不支持任何压缩格式

### common options

Sink 插件通用参数，请参考 [Sink 通用选项](../sink-common-options.md) 获取详细信息。

### max_rows_in_memory [int]

当文件格式为 Excel 时，内存中可以缓存的最大数据项数。

### sheet_name [string]

写入工作表的名称

### csv_string_quote_mode [string]

当文件格式为 CSV 时，CSV 的字符串引用模式。

- ALL: 所有字符串字段都会被引用。
- MINIMAL: 引用包含特殊字符的字段，如字段分隔符、引用字符或行分隔符字符串中的任何字符。
- NONE: 从不引用字段。当数据中出现分隔符时，打印机会在其前面加上转义字符。如果未设置转义字符，格式验证将抛出异常。

### xml_root_tag [string]

指定 XML 文件中根元素的标签名称。

### xml_row_tag [string]

指定 XML 文件中数据行的标签名称。

### xml_use_attr_format [boolean]

指定是否使用标签属性格式处理数据。

### parquet_avro_write_timestamp_as_int96 [boolean]

支持将时间戳写入 Parquet INT96，仅对 parquet 文件有效。

### parquet_avro_write_fixed_as_int96 [array]

支持将 12-byte 字段写入 Parquet INT96，仅对 parquet 文件有效。

### schema_save_mode[Enum]

在开启同步任务之前，对目标路径进行不同的处理。  
选项介绍：  
`RECREATE_SCHEMA` ：当路径不存在时创建。如果路径已存在，则删除路径并重新创建。         
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：当路径不存在时创建，路径存在时使用路径。        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当路径不存在时报错  
`IGNORE` ：忽略表的处理

### data_save_mode[Enum]

在开启同步任务之前，对目标路径中的数据文件进行不同的处理。
选项介绍：  
`DROP_DATA`：使用路径但删除路径中的数据文件。
`APPEND_DATA`：使用路径，并在路径中添加新文件以写入数据。   
`ERROR_WHEN_DATA_EXISTS`：当路径中存在数据文件时，将报错。

### encoding [string]

仅当 file_format_type 为 json,text,csv,xml 时使用。
写入文件的编码。此参数将由 `Charset.forName(encoding)` 解析。

## 示例

### 简单示例：

> 此示例定义了一个 SeaTunnel 同步任务，通过 FakeSource 自动生成数据并将其发送到 S3File Sink。FakeSource 总共生成 16 行数据 (row.num=16)，每行有两个字段，name (字符串类型) 和 age (int 类型)。最终的目标 s3 目录将创建一个文件，并将所有数据写入其中。
> 在运行此作业之前，您需要创建 s3 路径：/seatunnel/text。如果您尚未安装和部署 SeaTunnel，您需要按照 [安装 SeaTunnel](../../start-v2/locally/deployment.md) 中的说明安装和部署 SeaTunnel。然后按照 [使用 SeaTunnel Engine 快速入门](../../start-v2/locally/quick-start-seatunnel-engine.md) 中的说明运行此作业。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件，仅用于测试和演示功能源插件
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        name = string
        c_boolean = boolean
        age = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
# 如果您想了解更多关于如何配置SeaTunnel以及查看完整的源插件列表，
# 请访问 https://seatunnel.apache.org/docs/connector-v2/source
source {
}

transform {
  # 如果您想了解更多关于如何配置SeaTunnel以及查看完整的转换插件列表，
  # 请访问 https://seatunnel.apache.org/docs/transform-v2
}

sink {
    S3File {
      bucket = "s3a://seatunnel-test"
      tmp_path = "/tmp/seatunnel"
      path="/seatunnel/text"
      fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
      fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
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
      is_enable_transaction=true
      hadoop_s3_properties {
        "fs.s3a.buffer.dir" = "/data/st_test/s3a"
        "fs.s3a.fast.upload.buffer" = "disk"
      }
  }
  # 如果您想了解更多关于如何配置SeaTunnel以及查看完整的接收插件列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/sink
}
```

对于文本文件格式，包含 `have_partition`、`custom_filename`、`sink_columns` 和 `com.amazonaws.auth.InstanceProfileCredentialsProvider`

```hocon
S3File {
  bucket = "s3a://seatunnel-test"
  tmp_path = "/tmp/seatunnel"
  path="/seatunnel/text"
  fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
  fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
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
  is_enable_transaction=true
  hadoop_s3_properties {
    "fs.s3a.buffer.dir" = "/data/st_test/s3a"
    "fs.s3a.fast.upload.buffer" = "disk"
  }
}
```

对于Parquet文件格式，简单配置使用 `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`

```hocon
S3File {
  bucket = "s3a://seatunnel-test"
  tmp_path = "/tmp/seatunnel"
  path="/seatunnel/parquet"
  fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
  fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  access_key = "xxxxxxxxxxxxxxxxx"
  secret_key = "xxxxxxxxxxxxxxxxx"
  file_format_type = "parquet"
  hadoop_s3_properties {
    "fs.s3a.buffer.dir" = "/data/st_test/s3a"
    "fs.s3a.fast.upload.buffer" = "disk"
  }
}
```

对于ORC文件格式，简单配置使用 `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`

```hocon
S3File {
  bucket = "s3a://seatunnel-test"
  tmp_path = "/tmp/seatunnel"
  path="/seatunnel/orc"
  fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
  fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  access_key = "xxxxxxxxxxxxxxxxx"
  secret_key = "xxxxxxxxxxxxxxxxx"
  file_format_type = "orc"
  schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  data_save_mode="APPEND_DATA"
}
```

多表写入和保存模式

```hocon
env {
  "job.name"="SeaTunnel_job"
  "job.mode"=STREAMING
}
source {
  MySQL-CDC {
      database-names=[
          "wls_t1"
      ]
      table-names=[
          "wls_t1.mysqlcdc_to_s3_t3",
          "wls_t1.mysqlcdc_to_s3_t4",
          "wls_t1.mysqlcdc_to_s3_t5",
          "wls_t1.mysqlcdc_to_s3_t1",
          "wls_t1.mysqlcdc_to_s3_t2"
      ]
      password="xxxxxx"
      username="xxxxxxxxxxxxx"
      base-url="jdbc:mysql://localhost:3306/qa_source"
  }
}

transform {
}

sink {
  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel/${table_name}"
    path="/test/${table_name}"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "orc"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode="APPEND_DATA"
  }
}
```

### enable_header_write [boolean]
仅在 file_format_type 为 text 或 csv 时使用。false：不写入表头，true：写入表头。
