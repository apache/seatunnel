import ChangeLog from '../changelog/connector-file-sftp.md';

# SftpFile

> Sftp文件数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [多模态](../../concept/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的分片](../../concept/connector-v2-features.md)
- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown

## 描述

从sftp文件服务器读取数据。

## 支持的数据源信息

为了使用SftpFile连接器，需要以下依赖项。
可以通过install-plugin.sh或从Maven中央仓库下载。

| 数据源 | 支持的版本 |                                       依赖                                        |
|------------|--------------------|-----------------------------------------------------------------------------------------|
| SftpFile   | universal          | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-file-sftp) |

:::tip

如果您使用spark/flink，为了使用此连接器，您必须确保您的spark/flink集群已经集成了hadoop。测试过的hadoop版本是2.x。

如果您使用SeaTunnel引擎，它在您下载和安装SeaTunnel引擎时会自动集成hadoop jar。您可以检查${SEATUNNEL_HOME}/lib下的jar包来确认这一点。

为了支持更多文件类型，我们做了一些权衡，因此我们使用HDFS协议进行内部访问Sftp，此连接器需要一些hadoop依赖项。
它只支持hadoop版本**2.9.X+**。

:::

## 数据类型映射

文件没有特定的类型列表，我们可以通过在配置中指定Schema来指示相应的数据需要转换为哪种SeaTunnel数据类型。

| SeaTunnel数据类型 |
|---------------------|
| STRING              |
| SHORT               |
| INT                 |
| BIGINT              |
| BOOLEAN             |
| DOUBLE              |
| DECIMAL             |
| FLOAT               |
| DATE                |
| TIME                |
| TIMESTAMP           |
| BYTES               |
| ARRAY               |
| MAP                 |

## 数据源选项

| 名称                         | 类型      | 是否必需 | 默认值                 | 描述                                                                                                                                                                                                                                                 |
|----------------------------|---------|------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                       | String  | 是    | -                   | 目标sftp主机是必需的                                                                                                                                                                                                                                       |
| port                       | Int     | 是    | -                   | 目标sftp端口是必需的                                                                                                                                                                                                                                       |
| user                       | String  | 是    | -                   | 目标sftp用户名是必需的                                                                                                                                                                                                                                      |
| password                   | String  | 是    | -                   | 目标sftp密码是必需的                                                                                                                                                                                                                                       |
| path                       | String  | 是    | -                   | 源文件路径。                                                                                                                                                                                                                                             |
| file_format_type           | String  | 是    | -                   | 请查看下面的#file_format_type                                                                                                                                                                                                                            |
| file_filter_pattern        | String  | 否    | -                   | 过滤模式，用于过滤文件。                                                                                                                                                                                                                                       |
| filename_extension         | string  | 否    | -                   | 过滤文件名扩展名，用于过滤具有特定扩展名的文件。例如：`csv` `.txt` `json` `.xml`。                                                                                                                                                                                             |
| delimiter/field_delimiter  | String  | 否    | \001                | **delimiter**参数将在2.3.5版本后弃用，请使用**field_delimiter**代替。<br/> 字段分隔符，用于告诉连接器在读取文本文件时如何切分字段。<br/> 默认`\001`，与hive的默认分隔符相同                                                                                                                                |
| row_delimiter              | string  | 否    | \n                  | 行分隔符，用于告诉连接器在读取文本文件时如何切分行。<br/> 默认`\n`。                                                                                                                                                                                                            |                                                                                                                                                                                                           |
| parse_partition_from_path  | Boolean | 否    | true                | 控制是否从文件路径解析分区键和值<br/> 例如，如果您从路径`oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`读取文件<br/> 文件中的每条记录数据都将添加这两个字段：<br/>      name       age  <br/> tyrantlucifer  26   <br/> 提示：**不要在schema选项中定义分区字段**                            |
| date_format                | String  | 否    | yyyy-MM-dd          | 日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式：<br/> `yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` <br/> 默认`yyyy-MM-dd`                                                                                                                                                 |
| datetime_format            | String  | 否    | yyyy-MM-dd HH:mm:ss | 日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式：<br/> `yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss` <br/> 默认`yyyy-MM-dd HH:mm:ss`                                                                                        |
| time_format                | String  | 否    | HH:mm:ss            | 时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式：<br/> `HH:mm:ss` `HH:mm:ss.SSS` <br/> 默认`HH:mm:ss`                                                                                                                                                                |
| skip_header_row_number     | Long    | 否    | 0                   | 跳过前几行，但仅适用于txt和csv。<br/> 例如，设置如下：<br/> `skip_header_row_number = 2` <br/> 然后SeaTunnel将跳过源文件的前2行                                                                                                                                                    |
| read_columns               | list    | 否    | -                   | 数据源的读取列列表，用户可以使用它来实现字段投影。                                                                                                                                                                                                                          |
| sheet_name                 | String  | 否    | -                   | 读取工作簿的工作表，仅在file_format为excel时使用。                                                                                                                                                                                                                  |
| xml_row_tag                | string  | 否    | -                   | 指定XML文件中数据行的标签名称，仅在file_format为xml时使用。                                                                                                                                                                                                             |
| xml_use_attr_format        | boolean | 否    | -                   | 指定是否使用标签属性格式处理数据，仅在file_format为xml时使用。                                                                                                                                                                                                             |
| csv_use_header_line        | boolean | 否    | false               | 是否使用标题行来解析文件，仅在file_format为`csv`且文件包含符合RFC 4180的标题行时使用                                                                                                                                                                                             |
| schema                     | Config  | 否    | -                   | 请查看下面的#schema                                                                                                                                                                                                                                      |
| compress_codec             | String  | 否    | None                | 文件的压缩编解码器，支持的详细信息如下所示：<br/> - txt: `lzo` `None` <br/> - json: `lzo` `None` <br/> - csv: `lzo` `None` <br/> - orc: `lzo` `snappy` `lz4` `zlib` `None` <br/> - parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `None` <br/> 提示：excel类型不支持任何压缩格式 |
| archive_compress_codec     | string  | 否    | none                |
| encoding                   | string  | 否    | UTF-8               |
| null_format                | string  | 否    | -                   | 仅在file_format_type为text时使用。null_format用于定义哪些字符串可以表示为null。例如：`\N`                                                                                                                                                                                   |
| binary_chunk_size          | int     | 否    | 1024                | 仅在file_format_type为binary时使用。读取二进制文件的块大小（以字节为单位）。默认为1024字节。较大的值可能会提高大文件的性能，但会使用更多内存。                                                                                                                                                               |
| binary_complete_file_mode  | boolean | 否    | false               | 仅在file_format_type为binary时使用。是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为false。                                                                                                                                                                   |
| common-options             |         | 否    | -                   | 数据源插件通用参数，请参考[数据源通用选项](../source-common-options.md)了解详情。                                                                                                                                                                                           |
| file_filter_modified_start | string  | 否    | -                   | 按照最后修改时间过滤文件。 要过滤的开始时间(包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`                                                                                                                                                                                          |
| file_filter_modified_end   | string  | 否    | -                   | 按照最后修改时间过滤文件。 要过滤的结束时间(不包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`                                                                                                                                                                                         |

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

### file_format_type [string]

文件类型，支持以下文件类型：
`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`
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
如果您将文件类型指定为`parquet` `orc`，则不需要schema选项，连接器可以自动找到上游数据的schema。
如果您将文件类型指定为`text` `csv`，您可以选择指定schema信息或不指定。
例如，上游数据如下：

```text
tyrantlucifer#26#male
```

如果您不指定数据schema，连接器将把上游数据视为如下：
|        content        |
|-----------------------|
| tyrantlucifer#26#male |
如果您指定数据schema，除了CSV文件类型外，您还应该指定选项`field_delimiter`
您应该按如下方式指定schema和分隔符：

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

如果您将文件类型指定为`binary`，SeaTunnel可以同步任何格式的文件，
例如压缩包、图片等。简而言之，任何文件都可以同步到目标位置。

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
在此要求下，您需要确保源和接收器同时使用`binary`格式进行文件同步。

### compress_codec [string]

文件的压缩编解码器，支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:
  自动识别压缩类型，无需额外设置。

### archive_compress_codec [string]

归档文件的压缩编解码器，支持的详细信息如下所示：

| archive_compress_codec | file_format        | archive_compress_suffix |
|--------------------|--------------------|---------------------|
| ZIP                | txt,json,excel,xml | .zip                |
| TAR                | txt,json,excel,xml | .tar                |
| TAR_GZ             | txt,json,excel,xml | .tar.gz             |
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

### schema [config]

#### fields [Config]

上游数据的schema。

## 如何创建Sftp数据同步作业

以下示例演示如何创建从sftp读取数据并在本地客户端打印的数据同步作业：

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# 创建连接到sftp的数据源
source {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass
    path = "tmp/seatunnel/read/json"
    file_format_type = "json"
    plugin_output = "sftp"
    schema = {
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
}

# 控制台打印读取的sftp数据
sink {
  Console {
    parallelism = 1
  }
}
```
### 多表

```hocon

SftpFile {
  tables_configs = [
    {
      schema {
        table = "student"
        fields {
          name = string
          age = int
        }
      }
      path = "/tmp/seatunnel/sink/text"
      host = "192.168.31.48"
      port = 21
      user = tyrantlucifer
      password = tianchao
      file_format_type = "parquet"
    },
    {
      schema {
        table = "teacher"
        fields {
          name = string
          age = int
        }
      }
      path = "/tmp/seatunnel/sink/text"
      host = "192.168.31.48"
      port = 21
      user = tyrantlucifer
      password = tianchao
      file_format_type = "parquet"
    }
  ]
}

```

### 过滤文件

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass
    path = "tmp/seatunnel/read/json"
    file_format_type = "json"
    plugin_output = "sftp"
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
