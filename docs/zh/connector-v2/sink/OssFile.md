import ChangeLog from '../changelog/connector-file-oss.md';

# OssFile

> Oss 文件 sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖性

### 适用于Spark/Flink引擎

1. 您必须确保您的spark/flink集群已经集成了hadoop。测试的hadoop版本是2.x。
2. 您必须确保`${SEATUNNEL_HOME}/plugins/`目录中的`hadoop-aliyun-xx.jar`, `aliyun-sdk-oss-xx.jar`和`jdom-xx.jar`的版本与您在spark/flink中使用的hadoop版本匹配，`aliyun-sdk-oss-x.x.jar`和`jdom-xx.jar`版本需要与`hadoop-aliyun`版本对应的版本。例如:`hadoop-aliyun-3.1.4.jar`依赖项`aliyun-sdk-oss-3.4.1.jar`和`jdom-1.1.jar`。

### 适用于SeaTunnel Zeta引擎

1. 您必须确保在`${seatunnel_HOME}/lib/`目录中有`seatunnel-hadopp3-3.1.4-uber.jar `、`aliyun-sdk-oss-3.4.1.jar `、` hadoop-aliyun-3.1.4.jar`和`jdom-1.1.jar `。

## 关键特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [支持多表写入](../../concept/connector-v2-features.md)

默认情况下，我们使用2PC commit来确保`精确一次`

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## 数据类型映射

如果写入`csv`、`text`文件类型，则所有列将为字符串。

### Orc 文件类型

| SeaTunnel 数据类型  | Orc 数据类型         |
|----------------------|-----------------------|
| STRING               | STRING                |
| BOOLEAN              | BOOLEAN               |
| TINYINT              | BYTE                  |
| SMALLINT             | SHORT                 |
| INT                  | INT                   |
| BIGINT               | LONG                  |
| FLOAT                | FLOAT                 |
| FLOAT                | FLOAT                 |
| DOUBLE               | DOUBLE                |
| DECIMAL              | DECIMAL               |
| BYTES                | BINARY                |
| DATE                 | DATE                  |
| TIME <br/> TIMESTAMP | TIMESTAMP             |
| ROW                  | STRUCT                |
| NULL                 | 不支持的数据类型 |
| ARRAY                | LIST                  |
| Map                  | Map                   |

### Parquet 文件类型


| SeaTunnel 数据类型  | Parquet 数据类型     |
|----------------------|-----------------------|
| STRING               | STRING                |
| BOOLEAN              | BOOLEAN               |
| TINYINT              | INT_8                 |
| SMALLINT             | INT_16                |
| INT                  | INT32                 |
| BIGINT               | INT64                 |
| FLOAT                | FLOAT                 |
| FLOAT                | FLOAT                 |
| DOUBLE               | DOUBLE                |
| DECIMAL              | DECIMAL               |
| BYTES                | BINARY                |
| DATE                 | DATE                  |
| TIME <br/> TIMESTAMP | TIMESTAMP_MILLIS      |
| ROW                  | GroupType             |
| NULL                 | 不支持的数据类型      |
| ARRAY                | LIST                  |
| Map                  | Map                   |

## 选项

| 名称                                  | 类型    | 必需 | 默认值                                    | 描述                                                                                                                                                            |
|---------------------------------------|---------|----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | 是      | 写入文件的oss路径。             |                                                                                                                                                                        |
| tmp_path                              | string  | 否       | /tmp/seatunnel                             | 结果文件将首先写入tmp路径，然后使用`mv`将tmp-dir提交到目标dir。因此需要一个OSS目录。                                                      |
| bucket                                | string  | 是      | -                                          |                                                                                                                                                                        |
| access_key                            | string  | 是      | -                                          |                                                                                                                                                                        |
| access_secret                         | string  | 是      | -                                          |                                                                                                                                                                        |
| endpoint                              | string  | 是      | -                                          |                                                                                                                                                                        |
| custom_filename                       | boolean | 否       | false                                      | 是否需要自定义文件名                                                                                                                                   |
| file_name_expression                  | string  | 否       | "${transactionId}"                         | 仅在custom_filename为true时使用                                                                                                                                 |
| filename_time_format                  | string  | 否       | "yyyy.MM.dd"                               | 仅在custom_filename为true时使用                                                                                                                                 |
| file_format_type                      | string  | 否       | "csv"                                      |                                                                                                                                                                        |
| field_delimiter                       | string  | 否       | '\001'                                     | 仅当file_format_type为文本时使用                                                                                                                                |
| row_delimiter                         | string  | 否       | "\n"                                       | 仅当file_format_type为文本时使用                                                                                                                                |
| have_partition                        | boolean | 否       | false                                      | 是否需要处理分区。                                                                                                                                |
| partition_by                          | array   | 否       | -                                          | 只有在have_partition为true时才使用                                                                                                                                  |
| partition_dir_expression              | string  | 否       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 只有在have_partition为true时才使用                                                                                                                                 |
| is_partition_field_write_in_file      | boolean | 否       | false                                      | 只有在have_partition为true时才使用                                                                                                                                  |
| sink_columns                          | array   | 否       |                                            | 当此参数为空时，所有字段都是接收列                                                                                                              |
| is_enable_transaction                 | boolean | 否       | true                                       |                                                                                                                                                                        |
| batch_size                            | int     | 否       | 1000000                                    |                                                                                                                                                                        |
| compress_codec                        | string  | 否       | none                                       |                                                                                                                                                                        |
| common-options                        | object  | 否       | -                                          |                                                                                                                                                                        |
| max_rows_in_memory                    | int     | 否       | -                                          | 仅当file_format_type为excel时使用。                                                                                                                              |
| sheet_name                            | string  | 否       | Sheet${Random number}                      | 仅当file_format_type为excel时使用。                                                                                                                              |
| csv_string_quote_mode                 | enum    | 否       | MINIMAL                                    | 仅在file_format为csv时使用。                                                                                                                                     |
| xml_root_tag                          | string  | 否       | RECORDS                                    | 仅在file_format为xml时使用。                                                                                                                                    |
| xml_row_tag                           | string  | 否       | RECORD                                     | 仅在file_format为xml时使用。                                                                                                                                    |
| xml_use_attr_format                   | boolean | 否       | -                                          | 仅在file_format为xml时使用。                                                                                                                                     |
| single_file_mode                      | boolean | 否       | false                                      | 每个并行处理只会输出一个文件。启用此参数后，batch_size将不会生效。输出文件名没有文件块后缀。 |
| create_empty_file_when_no_data        | boolean | 否       | false                                      | 当上游没有数据同步时，仍然会生成相应的数据文件。                                                                     |
| parquet_avro_write_timestamp_as_int96 | boolean | 否       | false                                      | 仅在file_format为parquet时使用。                                                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | 否       | -                                          | 仅在file_format为parquet时使用。                                                                                                                                 |
| enable_header_write                   | boolean | 否       | false                                      | 仅当file_format_type为文本、csv时使用<br/>false：不写标头，true：写标头。                                                                          |
| encoding                              | string  | 否       | "UTF-8"                                    | 仅当file_format_type为json、text、csv、xml时使用。                                                                                                                 |

### path [string]

目标目录路径是必需的。

### bucket [string]

oss文件系统的bucket地址，例如：`oss://tyrantlucifer-image-bed`

### access_key [string]

oss文件系统的access_key。

### access_secret [string]

oss文件系统的access_secret。

### endpoint [string]

oss文件系统的endpoint端点。

### custom_filename [boolean]

是否自定义文件名

### file_name_expression [string]

仅在`custom_filename`为`true`时使用

`file_name_expression描述了将在`path`中创建的文件表达式。我们可以在`file_name_expression`中添加变量`${now}`或`${uuid}`，类似于`test_${uuid}_${now}`，`${now}`表示当前时间，其格式可以通过指定选项`filename_time_format`来定义。

请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。

### filename_time_format [String]

仅在`custom_filename`为`true`时使用`

当`file_name_expression`参数中的格式为`xxxx-${Now}时，`filename_time_format`可以指定路径的时间格式，默认值为`yyyy.MM.dd。常用的时间格式如下：

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

### file_format_type [string]

我们支持以下文件类型:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

请注意，最终文件名将以file_format_type的后缀结尾，文本文件的后缀为`txt`。

### field_delimiter [string]

数据行中列之间的分隔符。只需要`文本`文件格式。

### row_delimiter [string]

文件中行之间的分隔符。只需要`text`文件格式。

### have_partition [boolean]

是否需要处理分区。

### partition_by [array]

仅当`have_partition`为`true`时使用。

根据所选字段对数据进行分区。

### partition_dir_expression [string]

仅在`have_partition`为`true`时使用。

如果指定了`partition_by`，我们将根据分区信息生成相应的分区目录，并将最终文件放置在分区目录中。

默认的`partition_dir_expression`是`${k0}=${v0}/${k1}=${1v1}//${kn}=${vn}/``k0是第一个分区字段，v0是第一个划分字段的值。

### is_partition_field_write_in_file [boolean]

仅在`have_partition`为`true`时使用。

如果`is_partition_field_write_in_file`为`true`，则分区字段及其值将写入数据文件。

例如，如果你想写一个Hive数据文件，它的值应该是`false`。

### sink_columns [array]

哪些列需要写入文件，默认值是从`Transform`或`Source`获取的所有列。
字段的顺序决定了文件实际写入的顺序。

### is_enable_transaction [boolean]

如果`is_enable_transaction`为true，我们将确保数据在写入目标目录时不会丢失或重复。

请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。

现在只支持`true`。

### batch_size [int]

文件中的最大行数。对于SeaTunnel引擎，文件中的行数由`batch_size`和`checkpoint.interval`共同决定。如果`checkpoint.interval`的值足够大，sink writer将在文件中写入行，直到文件中的行大于`batch_size`。如果`checkpoint.interval`较小，则接收器写入程序将在新的检查点触发时创建一个新文件。

### compress_codec [string]

文件的压缩编解码器和支持的详细信息如下所示:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

提示：excel类型不支持任何压缩格式

### 通用选项

Sink插件常用参数，请参考[Sink common Options]（../Sink common Options.md）了解详细信息。

### max_rows_in_memory [int]

当文件格式为Excel时，内存中可以缓存的最大数据项数。

### sheet_name [string]

编写工作簿的工作表

### csv_string_quote_mode [string]

当文件格式为CSV时，CSV的字符串引用模式。

- ALL: 所有字符串字段都将被引用。
- MINIMAL: 引号字段包含特殊字符，如字段分隔符、引号字符或行分隔符字符串中的任何字符。
- NONE: 从不引用字段。当分隔符出现在数据中时，打印机会用转义符作为前缀。如果未设置转义符，格式验证将抛出异常。

### xml_root_tag [string]

指定XML文件中根元素的标记名。

### xml_row_tag [string]

指定XML文件中数据行的标记名称。

### xml_use_attr_format [boolean]

指定是否使用标记属性格式处理数据。

### parquet_avro_write_timestamp_as_int96 [boolean]

支持从时间戳写入Parquet INT96，仅适用于拼花地板文件。

### parquet_avro_write_fixed_as_int96 [array]

支持从12-byte字段写入Parquet INT96，仅适用于拼花地板文件。

### encoding [string]

仅当file_format_type为json、text、csv、xml时使用。
要写入的文件的编码。此参数将由`Charset.forName（encoding）`解析。

## 如何创建Oss数据同步作业


以下示例演示了如何创建从假数据源读取数据并写入的数据同步作业
把它发送到Oss：

对于具有`have_partition`、`custom_filename`和`sink_columns`的文本文件格式

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# 创建产品数据源
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# 将数据写入Oss
sink {
  OssFile {
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

适用于带有`have_partition`和`sink_columns`的parquet文件格式

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# 将数据写入Oss
sink {
  OssFile {
    path = "/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }
}
```

对于orc文件格式的简单配置

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# 将数据写入Oss
sink {
  OssFile {
    path="/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
  }
}
```

### enable_header_write [boolean]

仅当file_format_type为`text` `csv`时使用。false：不写标头，true：写标头。

### 多表

用于从上游提取source元数据, 您可以在路径中使用`${database_name}`, `${table_name}` 和 `${schema_name}`。

```bash

env {
  parallelism = 1
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
       {
        schema = {
          table = "fake1"
          fields {
            c_map = "map<string, string>"
            c_array = "array<int>"
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_bytes = bytes
            c_date = date
            c_decimal = "decimal(38, 18)"
            c_timestamp = timestamp
            c_row = {
              c_map = "map<string, string>"
              c_array = "array<int>"
              c_string = string
              c_boolean = boolean
              c_tinyint = tinyint
              c_smallint = smallint
              c_int = int
              c_bigint = bigint
              c_float = float
              c_double = double
              c_bytes = bytes
              c_date = date
              c_decimal = "decimal(38, 18)"
              c_timestamp = timestamp
            }
          }
        }
       },
       {
       schema = {
         table = "fake2"
         fields {
           c_map = "map<string, string>"
           c_array = "array<int>"
           c_string = string
           c_boolean = boolean
           c_tinyint = tinyint
           c_smallint = smallint
           c_int = int
           c_bigint = bigint
           c_float = float
           c_double = double
           c_bytes = bytes
           c_date = date
           c_decimal = "decimal(38, 18)"
           c_timestamp = timestamp
           c_row = {
             c_map = "map<string, string>"
             c_array = "array<int>"
             c_string = string
             c_boolean = boolean
             c_tinyint = tinyint
             c_smallint = smallint
             c_int = int
             c_bigint = bigint
             c_float = float
             c_double = double
             c_bytes = bytes
             c_date = date
             c_decimal = "decimal(38, 18)"
             c_timestamp = timestamp
           }
         }
       }
      }
    ]
  }
}

sink {
  OssFile {
    bucket = "oss://whale-ops"
    access_key = "xxxxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxx"
    endpoint = "https://oss-accelerate.aliyuncs.com"
    path = "/tmp/fake_empty/text/${table_name}"
    row_delimiter = "\n"
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_name_expression = "${transactionId}_${now}"
    file_format_type = "text"
    filename_time_format = "yyyy.MM.dd"
    is_enable_transaction = true
    compress_codec = "lzo"
  }
}
```

### 提示

> 1.[SeaTunnel部署方案](../../start-v2/locally/deployment.md).

## 变更日志

<ChangeLog />