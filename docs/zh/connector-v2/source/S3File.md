import ChangeLog from '../changelog/connector-file-s3.md';

# S3File

> S3文件数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

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

## 描述

从aws s3文件系统读取数据。

## 支持的数据源信息

| 数据源 | 支持的版本 |
|------------|--------------------|
| S3         | current            |

## 依赖

> 如果您使用spark/flink，为了使用此连接器，您必须确保您的spark/flink集群已经集成了hadoop。测试过的hadoop版本是2.x。<br/>
>
> 如果您使用SeaTunnel Zeta，它在您下载和安装SeaTunnel Zeta时会自动集成hadoop jar。您可以检查${SEATUNNEL_HOME}/lib下的jar包来确认这一点。<br/>
> 要使用此连接器，您需要将hadoop-aws-3.1.4.jar和aws-java-sdk-bundle-1.12.692.jar放在${SEATUNNEL_HOME}/lib目录中。

## 数据类型映射

数据类型映射与正在读取的文件类型相关，我们支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml`

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

| 名称                              | 类型      | 是否必需 | 默认值                                                   | 描述                                                                                                                                                                                                                                                                                                                    |
|---------------------------------|---------|------|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                            | string  | 是    | -                                                     | 需要读取的s3路径，可以有子路径，但子路径需要满足一定的格式要求。具体要求可以参考"parse_partition_from_path"选项                                                                                                                                                                                                                                                |
| file_format_type                | string  | 是    | -                                                     | 文件类型，支持以下文件类型：`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`                                                                                                                                                                                                                                   |
| bucket                          | string  | 是    | -                                                     | s3文件系统的bucket地址，例如：`s3n://seatunnel-test`，如果您使用`s3a`协议，此参数应为`s3a://seatunnel-test`。                                                                                                                                                                                                                                   |
| fs.s3a.endpoint                 | string  | 是    | -                                                     | fs s3a端点                                                                                                                                                                                                                                                                                                              |
| fs.s3a.aws.credentials.provider | string  | 是    | com.amazonaws.auth.InstanceProfileCredentialsProvider | s3a的认证方式。我们目前只支持`org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`和`com.amazonaws.auth.InstanceProfileCredentialsProvider`。有关凭据提供程序的更多信息，您可以查看[Hadoop AWS文档](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Simple_name.2Fsecret_credentials_with_SimpleAWSCredentialsProvider.2A) |
| read_columns                    | list    | 否    | -                                                     | 数据源的读取列列表，用户可以使用它来实现字段投影。支持列投影的文件类型如下所示：`text` `csv` `parquet` `orc` `json` `excel` `xml`。如果用户想在读取`text` `json` `csv`文件时使用此功能，必须配置"schema"选项。                                                                                                                                                                         |
| access_key                      | string  | 否    | -                                                     | 仅在`fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`时使用                                                                                                                                                                                                                        |
| secret_key                      | string  | 否    | -                                                     | 仅在`fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`时使用                                                                                                                                                                                                                        |
| hadoop_s3_properties            | map     | 否    | -                                                     | 如果您需要添加其他选项，可以在此处添加并参考此[链接](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)                                                                                                                                                                                                             |
| delimiter/field_delimiter       | string  | 否    | \001                                                  | 字段分隔符，用于告诉连接器在读取文本文件时如何切分字段。默认`\001`，与hive的默认分隔符相同。                                                                                                                                                                                                                                                                   |
| row_delimiter                   | string  | 否    | \n                                                    | 行分隔符，用于告诉连接器在读取文本文件时如何切分行。默认`\n`。                                                                                                                                                                                                                                                                                     |                                                                                                                                                                                                                                                                               |
| parse_partition_from_path       | boolean | 否    | true                                                  | 控制是否从文件路径解析分区键和值。例如，如果您从路径`s3n://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`读取文件。文件中的每条记录数据都将添加这两个字段：name="tyrantlucifer"，age=16                                                                                                                                                                  |
| date_format                     | string  | 否    | yyyy-MM-dd                                            | 日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式：`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`。默认`yyyy-MM-dd`                                                                                                                                                                                                                                |
| datetime_format                 | string  | 否    | yyyy-MM-dd HH:mm:ss                                   | 日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式：`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`                                                                                                                                                                                               |
| time_format                     | string  | 否    | HH:mm:ss                                              | 时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式：`HH:mm:ss` `HH:mm:ss.SSS`                                                                                                                                                                                                                                                            |
| skip_header_row_number          | long    | 否    | 0                                                     | 跳过前几行，但仅适用于txt和csv。例如，设置如下：`skip_header_row_number = 2`。然后SeaTunnel将跳过源文件的前2行                                                                                                                                                                                                                                         |
| csv_use_header_line             | boolean | 否    | false                                                 | 是否使用标题行来解析文件，仅在file_format为`csv`且文件包含符合RFC 4180的标题行时使用                                                                                                                                                                                                                                                                |
| schema                          | config  | 否    | -                                                     | 上游数据的schema。                                                                                                                                                                                                                                                                                                          |
| sheet_name                      | string  | 否    | -                                                     | 读取工作簿的工作表，仅在file_format为excel时使用。                                                                                                                                                                                                                                                                                     |
| xml_row_tag                     | string  | 否    | -                                                     | 指定XML文件中数据行的标签名称，仅对XML文件有效。                                                                                                                                                                                                                                                                                           |
| xml_use_attr_format             | boolean | 否    | -                                                     | 指定是否使用标签属性格式处理数据，仅对XML文件有效。                                                                                                                                                                                                                                                                                           |
| compress_codec                  | string  | 否    | none                                                  |                                                                                                                                                                                                                                                                                                                       |
| archive_compress_codec          | string  | 否    | none                                                  |                                                                                                                                                                                                                                                                                                                       |
| encoding                        | string  | 否    | UTF-8                                                 |                                                                                                                                                                                                                                                                                                                       |
| null_format                     | string  | 否    | -                                                     | 仅在file_format_type为text时使用。null_format用于定义哪些字符串可以表示为null。例如：`\N`                                                                                                                                                                                                                                                      |
| binary_chunk_size               | int     | 否    | 1024                                                  | 仅在file_format_type为binary时使用。读取二进制文件的块大小（以字节为单位）。默认为1024字节。较大的值可能会提高大文件的性能，但会使用更多内存。                                                                                                                                                                                                                                  |
| binary_complete_file_mode       | boolean | 否    | false                                                 | 仅在file_format_type为binary时使用。是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为false。                                                                                                                                                                                                                                      |
| file_filter_pattern             | string  | 否    |                                                       | 过滤模式，用于过滤文件。                                                                                                                                                                                                                                                                                                          |
| filename_extension              | string  | 否    | -                                                     | 过滤文件名扩展名，用于过滤具有特定扩展名的文件。例如：`csv` `.txt` `json` `.xml`。                                                                                                                                                                                                                                                                |
| common-options                  |         | 否    | -                                                     | 数据源插件通用参数，请参考[数据源通用选项](../source-common-options.md)了解详情。                                                                                                                                                                                                                                                              |

### delimiter/field_delimiter [string]

**delimiter**参数将在2.3.5版本后弃用，请使用**field_delimiter**代替。

### row_delimiter [string]

仅在 file_format 为 text 时需要配置。

行分隔符，用于告诉连接器如何分割行。

默认 `\n`。

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

### compress_codec [string]

文件的压缩编解码器，支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:
  自动识别压缩类型，无需额外设置。

### archive_compress_codec [string]

归档文件的压缩编解码器，支持的详细信息如下所示：

| archive_compress_codec | file_format | archive_compress_suffix |
|------------------------|------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

注意：gz压缩的excel文件需要压缩原始文件或指定文件后缀，例如e2e.xls ->e2e_test.xls.gz

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

## 示例

1. 在此示例中，我们从s3路径`s3a://seatunnel-test/seatunnel/text`读取数据，此路径中的文件类型是orc。
   我们使用`org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`进行身份验证，因此需要`access_key`和`secret_key`。
   文件中的所有列都将被读取并发送到接收器。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/text"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    file_format_type = "orc"
  }
}

transform {
  # 如果您想获取有关如何配置seatunnel和查看转换插件完整列表的更多信息，
    # 请访问 https://seatunnel.apache.org/docs/transform-v2
}

sink {
  Console {}
}
```

2. 使用`InstanceProfileCredentialsProvider`进行身份验证
   S3中的文件类型是json，因此需要配置schema选项。

```hocon

  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    schema {
      fields {
        id = int
        name = string
      }
    }
  }

```

3. 使用`InstanceProfileCredentialsProvider`进行身份验证
   S3中的文件类型是json，有五个字段（`id`、`name`、`age`、`sex`、`type`），因此需要配置schema选项。
   在此作业中，我们只需要将`id`和`name`列发送到mysql。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    read_columns = ["id", "name"]
    schema {
      fields {
        id = int
        name = string
        age = int
        sex = int
        type = string
      }
    }
  }
}

transform {
  # 如果您想获取有关如何配置seatunnel和查看转换插件完整列表的更多信息，
    # 请访问 https://seatunnel.apache.org/docs/transform-v2
}

sink {
  Console {}
}
```

### 过滤文件

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    read_columns = ["id", "name"]
    // 文件示例 abcD2024.csv
    file_filter_pattern = "abc[DX]*.*"
  }
}

sink {
  Console {
  }
}
```

## 变更日志

<ChangeLog />
