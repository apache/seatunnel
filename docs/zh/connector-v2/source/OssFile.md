import ChangeLog from '../changelog/connector-file-oss.md';

# OssFile

> Oss文件数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖

### 对于Spark/Flink引擎

1. 您必须确保您的spark/flink集群已经集成了hadoop。测试过的hadoop版本是2.x。
2. 您必须确保`hadoop-aliyun-xx.jar`、`aliyun-sdk-oss-xx.jar`和`jdom-xx.jar`在`${SEATUNNEL_HOME}/plugins/`目录中，并且`hadoop-aliyun` jar的版本需要与您在spark/flink中使用的hadoop版本相等，`aliyun-sdk-oss-xx.jar`和`jdom-xx.jar`版本需要是与`hadoop-aliyun`版本对应的版本。例如：`hadoop-aliyun-3.1.4.jar`依赖`aliyun-sdk-oss-3.4.1.jar`和`jdom-1.1.jar`。

### 对于SeaTunnel Zeta引擎

1. 您必须确保`seatunnel-hadoop3-3.1.4-uber.jar`、`aliyun-sdk-oss-3.4.1.jar`、`hadoop-aliyun-3.1.4.jar`和`jdom-1.1.jar`在`${SEATUNNEL_HOME}/lib/`目录中。

## 主要特性

- [x] [多模态](../../concept/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)

  在一次pollNext调用中读取分片中的所有数据。将读取的分片保存在快照中。

- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的分片](../../concept/connector-v2-features.md)
- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown

## 数据类型映射

数据类型映射与正在读取的文件类型相关，我们支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `markdown`

### JSON文件类型

如果您将文件类型指定为`json`，您还应该指定schema选项来告诉连接器如何将数据解析为您想要的行。

例如：

上游数据如下：

```json

{"code":  200, "data":  "get success", "success":  true}

```

您也可以在一个文件中保存多条数据，并用换行符分隔：

```json lines

{"code":  200, "data":  "get success", "success":  true}
{"code":  300, "data":  "get failed", "success":  false}

```

您应该按如下方式指定schema：

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

连接器将生成如下数据：

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

### 文本或CSV文件类型

如果您将`file_format_type`设置为`text`、`excel`、`csv`、`xml`。那么需要设置`schema`字段来告诉连接器如何将数据解析为行。

如果您设置了`schema`字段，您还应该设置选项`field_delimiter`，除非`file_format_type`是`csv`、`xml`、`excel`

您可以按如下方式设置schema和分隔符：

```hocon

field_delimiter = "#"
schema {
    fields {
        name = string
        age = int
        gender = string 
    }
}

```

连接器将生成如下数据：

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

### Orc文件类型

如果您将文件类型指定为`parquet` `orc`，则不需要schema选项，连接器可以自动找到上游数据的schema。

|          Orc数据类型           |                      SeaTunnel数据类型                       |
|----------------------------------|----------------------------------------------------------------|
| BOOLEAN                          | BOOLEAN                                                        |
| INT                              | INT                                                            |
| BYTE                             | BYTE                                                           |
| SHORT                            | SHORT                                                          |
| LONG                             | LONG                                                           |
| FLOAT                            | FLOAT                                                          |
| DOUBLE                           | DOUBLE                                                         |
| BINARY                           | BINARY                                                         |
| STRING<br/>VARCHAR<br/>CHAR<br/> | STRING                                                         |
| DATE                             | LOCAL_DATE_TYPE                                                |
| TIMESTAMP                        | LOCAL_DATE_TIME_TYPE                                           |
| DECIMAL                          | DECIMAL                                                        |
| LIST(STRING)                     | STRING_ARRAY_TYPE                                              |
| LIST(BOOLEAN)                    | BOOLEAN_ARRAY_TYPE                                             |
| LIST(TINYINT)                    | BYTE_ARRAY_TYPE                                                |
| LIST(SMALLINT)                   | SHORT_ARRAY_TYPE                                               |
| LIST(INT)                        | INT_ARRAY_TYPE                                                 |
| LIST(BIGINT)                     | LONG_ARRAY_TYPE                                                |
| LIST(FLOAT)                      | FLOAT_ARRAY_TYPE                                               |
| LIST(DOUBLE)                     | DOUBLE_ARRAY_TYPE                                              |
| Map<K,V>                         | MapType，K和V的类型将转换为SeaTunnel类型 |
| STRUCT                           | SeaTunnelRowType                                               |

### Parquet文件类型

如果您将文件类型指定为`parquet` `orc`，则不需要schema选项，连接器可以自动找到上游数据的schema。

| Parquet数据类型    | SeaTunnel数据类型                                            |
|----------------------|----------------------------------------------------------------|
| INT_8                | BYTE                                                           |
| INT_16               | SHORT                                                          |
| DATE                 | DATE                                                           |
| TIMESTAMP_MILLIS     | TIMESTAMP                                                      |
| INT64                | LONG                                                           |
| INT96                | TIMESTAMP                                                      |
| BINARY               | BYTES                                                          |
| FLOAT                | FLOAT                                                          |
| DOUBLE               | DOUBLE                                                         |
| BOOLEAN              | BOOLEAN                                                        |
| FIXED_LEN_BYTE_ARRAY | TIMESTAMP<br/> DECIMAL                                         |
| DECIMAL              | DECIMAL                                                        |
| LIST(STRING)         | STRING_ARRAY_TYPE                                              |
| LIST(BOOLEAN)        | BOOLEAN_ARRAY_TYPE                                             |
| LIST(TINYINT)        | BYTE_ARRAY_TYPE                                                |
| LIST(SMALLINT)       | SHORT_ARRAY_TYPE                                               |
| LIST(INT)            | INT_ARRAY_TYPE                                                 |
| LIST(BIGINT)         | LONG_ARRAY_TYPE                                                |
| LIST(FLOAT)          | FLOAT_ARRAY_TYPE                                               |
| LIST(DOUBLE)         | DOUBLE_ARRAY_TYPE                                              |
| Map<K,V>             | MapType，K和V的类型将转换为SeaTunnel类型 |
| STRUCT               | SeaTunnelRowType                                               |

## 选项

| 名称                         | 类型      | 是否必需 | 默认值                 | 描述                                                                                                                                                   |
|----------------------------|---------|------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                       | string  | 是    | -                   | 需要读取的Oss路径，可以有子路径，但子路径需要满足一定的格式要求。具体要求可以参考"parse_partition_from_path"选项                                                                              |
| file_format_type           | string  | 是    | -                   | 文件类型，支持以下文件类型：`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`                                                                  |
| bucket                     | string  | 是    | -                   | oss文件系统的bucket地址，例如：`oss://seatunnel-test`。                                                                                                          |
| endpoint                   | string  | 是    | -                   | fs oss端点                                                                                                                                             |
| read_columns               | list    | 否    | -                   | 数据源的读取列列表，用户可以使用它来实现字段投影。支持列投影的文件类型如下所示：`text` `csv` `parquet` `orc` `json` `excel` `xml`。如果用户想在读取`text` `json` `csv`文件时使用此功能，必须配置"schema"选项。        |
| access_key                 | string  | 否    | -                   |                                                                                                                                                      |
| access_secret              | string  | 否    | -                   |                                                                                                                                                      |
| delimiter                  | string  | 否    | \001                | 字段分隔符，用于告诉连接器在读取文本文件时如何切分字段。默认`\001`，与hive的默认分隔符相同。                                                                                                  |
| row_delimiter              | string  | 否    | \n                  | 行分隔符，用于告诉连接器在读取文本文件时如何切分行。默认`\n`。                                                                                                                    |
| parse_partition_from_path  | boolean | 否    | true                | 控制是否从文件路径解析分区键和值。例如，如果您从路径`oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`读取文件。文件中的每条记录数据都将添加这两个字段：name="tyrantlucifer"，age=16 |
| date_format                | string  | 否    | yyyy-MM-dd          | 日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式：`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`。默认`yyyy-MM-dd`                                                               |
| datetime_format            | string  | 否    | yyyy-MM-dd HH:mm:ss | 日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式：`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`                              |
| time_format                | string  | 否    | HH:mm:ss            | 时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式：`HH:mm:ss` `HH:mm:ss.SSS`                                                                                           |
| filename_extension         | string  | 否    | -                   | 过滤文件名扩展名，用于过滤具有特定扩展名的文件。例如：`csv` `.txt` `json` `.xml`。                                                                                               |
| skip_header_row_number     | long    | 否    | 0                   | 跳过前几行，但仅适用于txt和csv。例如，设置如下：`skip_header_row_number = 2`。然后SeaTunnel将跳过源文件的前2行                                                                        |
| csv_use_header_line        | boolean | 否    | false               | 是否使用标题行来解析文件，仅在file_format为`csv`且文件包含符合RFC 4180的标题行时使用                                                                                               |
| schema                     | config  | 否    | -                   | 上游数据的schema。                                                                                                                                         |
| sheet_name                 | string  | 否    | -                   | 读取工作簿的工作表，仅在file_format为excel时使用。                                                                                                                    |
| xml_row_tag                | string  | 否    | -                   | 指定XML文件中数据行的标签名称，仅在file_format为xml时使用。                                                                                                               |
| xml_use_attr_format        | boolean | 否    | -                   | 指定是否使用标签属性格式处理数据，仅在file_format为xml时使用。                                                                                                               |
| compress_codec             | string  | 否    | none                | 文件使用的压缩编解码器。                                                                                                                                         |
| encoding                   | string  | 否    | UTF-8               |
| null_format                | string  | 否    | -                   | 仅在file_format_type为text时使用。null_format用于定义哪些字符串可以表示为null。例如：`\N`                                                                                     |
| binary_chunk_size          | int     | 否    | 1024                | 仅在file_format_type为binary时使用。读取二进制文件的块大小（以字节为单位）。默认为1024字节。较大的值可能会提高大文件的性能，但会使用更多内存。                                                                 |
| binary_complete_file_mode  | boolean | 否    | false               | 仅在file_format_type为binary时使用。是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为false。                                                                     |
| file_filter_pattern        | string  | 否    |                     | 过滤模式，用于过滤文件。                                                                                                                                         |
| common-options             | config  | 否    | -                   | 数据源插件通用参数，请参考[数据源通用选项](../source-common-options.md)了解详情。                                                                                             |
| file_filter_modified_start | string  | 否    | -                   | 按照最后修改时间过滤文件。 要过滤的开始时间(包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`                                                                                            |
| file_filter_modified_end   | string  | 否    | -                   | 按照最后修改时间过滤文件。 要过滤的结束时间(不包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`                                                                                           |

### compress_codec [string]

文件的压缩编解码器，支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:
  自动识别压缩类型，无需额外设置。

### encoding [string]

仅在file_format_type为json、text、csv、xml时使用。
要读取的文件的编码。此参数将由`Charset.forName(encoding)`解析。

### binary_chunk_size [int]

仅在file_format_type为binary时使用。

读取二进制文件的块大小（以字节为单位）。默认为1024字节。较大的值可能会提高大文件的性能，但会使用更多内存。

### binary_complete_file_mode [boolean]

仅在file_format_type为binary时使用。

是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为false。

### file_format_type [string]

文件类型，支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`

如果您将文件类型指定为 `markdown`，SeaTunnel 可以解析 markdown 文件并提取结构化数据。
markdown 解析器提取各种元素，包括标题、段落、列表、代码块、表格等。
每个元素都转换为具有以下架构的行：
- `element_id`：元素的唯一标识符
- `element_type`：元素类型（Heading、Paragraph、ListItem 等）
- `heading_level`：标题级别（1-6，非标题元素为 null）
- `text`：元素的文本内容
- `page_number`：页码（默认：1）
- `position_index`：文档中的位置索引
- `parent_id`：父元素的 ID
- `child_ids`：子元素 ID 的逗号分隔列表

注意：Markdown 格式仅支持读取，不支持写入。

### file_filter_pattern [string]

过滤模式，用于过滤文件。

该模式遵循标准正则表达式。详情请参考 https://en.wikipedia.org/wiki/Regular_expression。
以下是一些示例。

文件结构示例：
```
/data/seatunnel/20241001/report.txt
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
/data/seatunnel/20241012/logo.png
```
匹配规则示例：

**示例1**：*匹配所有.txt文件*，正则表达式：
```
/data/seatunnel/20241001/.*\.txt
```
此示例匹配的结果是：
```
/data/seatunnel/20241001/report.txt
```
**示例2**：*匹配所有以abc开头的文件*，正则表达式：
```
/data/seatunnel/20241002/abc.*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**示例3**：*匹配所有以abc开头，且第四个字符是h或g的文件*，正则表达式：
```
/data/seatunnel/20241007/abc[h,g].*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
```
**示例4**：*匹配以202410开头的第三级文件夹和以.csv结尾的文件*，正则表达式：
```
/data/seatunnel/202410\d*/.*\.csv
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### schema [config]

仅在file_format_type为text、json、excel、xml或csv时需要配置（或其他我们无法从元数据读取schema的格式）。

#### fields [Config]

上游数据的schema。

## 如何创建Oss数据同步作业

以下示例演示如何创建从Oss读取数据并在本地客户端打印的数据同步作业：

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# 创建连接到Oss的数据源
source {
  OssFile {
    path = "/seatunnel/orc"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
  }
}

# 控制台打印读取的Oss数据
sink {
  Console {
  }
}
```

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# 创建连接到Oss的数据源
source {
  OssFile {
    path = "/seatunnel/json"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "json"
    schema {
      fields {
        id = int
        name = string
      }
    }
  }
}

# 控制台打印读取的Oss数据
sink {
  Console {
  }
}
```

### 多表

无需配置schema文件类型，例如：`orc`。

```
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
  OssFile {
    tables_configs = [
      {
          schema = {
              table = "fake01"
          }
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/orc"
          file_format_type = "orc"
      },
      {
          schema = {
              table = "fake02"
          }
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/orc"
          file_format_type = "orc"
      }
    ]
    plugin_output = "fake"
  }
}

sink {
  Assert {
    rules {
        table-names = ["fake01", "fake02"]
    }
  }
}
```

需要配置schema文件类型，例如：`json`

```

env {
  execution.parallelism = 1
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
  job.mode = "BATCH"
}

source {
  OssFile {
    tables_configs = [
      {
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/json"
          file_format_type = "json"
          schema = {
            table = "fake01"
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
                C_MAP = "map<string, string>"
                C_ARRAY = "array<int>"
                C_STRING = string
                C_BOOLEAN = boolean
                C_TINYINT = tinyint
                C_SMALLINT = smallint
                C_INT = int
                C_BIGINT = bigint
                C_FLOAT = float
                C_DOUBLE = double
                C_BYTES = bytes
                C_DATE = date
                C_DECIMAL = "decimal(38, 18)"
                C_TIMESTAMP = timestamp
              }
            }
          }
      },
      {
          bucket = "oss://whale-ops"
          access_key = "xxxxxxxxxxxxxxxxxxx"
          access_secret = "xxxxxxxxxxxxxxxxxxx"
          endpoint = "https://oss-accelerate.aliyuncs.com"
          path = "/test/seatunnel/read/json"
          file_format_type = "json"
          schema = {
            table = "fake02"
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
                C_MAP = "map<string, string>"
                C_ARRAY = "array<int>"
                C_STRING = string
                C_BOOLEAN = boolean
                C_TINYINT = tinyint
                C_SMALLINT = smallint
                C_INT = int
                C_BIGINT = bigint
                C_FLOAT = float
                C_DOUBLE = double
                C_BYTES = bytes
                C_DATE = date
                C_DECIMAL = "decimal(38, 18)"
                C_TIMESTAMP = timestamp
              }
            }
          }
      }
    ]
    plugin_output = "fake"
  }
}

sink {
  Assert {
    rules {
      table-names = ["fake01", "fake02"]
    }
  }
}
```

### 过滤文件

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OssFile {
    path = "/seatunnel/orc"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
    // 文件示例 abcD2024.csv
    file_filter_pattern = "abc[DX]*.*"
    // 筛选最后修改日期在 20240101 和 20240105 (不包括该日期) 之间的文件
    file_filter_modified_start = "2024-01-01 00:00:00"
    file_filter_modified_end = "2024-01-05 00:00:00"
  }
}

sink {
  Console {
  }
}
```

## 变更日志

<ChangeLog />
