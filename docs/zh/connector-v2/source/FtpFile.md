import ChangeLog from '../changelog/connector-file-ftp.md';

# FtpFile

> Ftp 文件 Source 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [多模态](../../concept/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次处理](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)
- [x] 文件格式类型
  - [x] 文本
  - [x] CSV
  - [x] JSON
  - [x] Excel
  - [x] XML
  - [x] 二进制

## 描述

从 FTP 文件服务器读取数据。

:::提示

如果您使用 Spark/Flink，为了使用此连接器，您必须确保您的 Spark/Flink 集群已经集成了 Hadoop。测试的 Hadoop 版本为 2.x。
如果您使用 SeaTunnel Engine，当您下载并安装 SeaTunnel Engine 时，它会自动集成 Hadoop 的 jar 包。您可以在 `${SEATUNNEL_HOME}/lib` 目录下检查 jar 包以确认这一点。

:::

## 配置项

| 名称                          | 类型      | 是否必填 | 默认值                 |
|-----------------------------|---------|------|---------------------|
| host                        | string  | 是    | -                   |
| port                        | int     | 是    | -                   |
| user                        | string  | 是    | -                   |
| password                    | string  | 是    | -                   |
| path                        | string  | 是    | -                   |
| file_format_type            | string  | 是    | -                   |
| connection_mode             | string  | 否    | active_local        |
| remote_verification_enabled | boolean | no   | true                |
| delimiter/field_delimiter   | string  | 否    | \001                |
| read_columns                | list    | 否    | -                   |
| parse_partition_from_path   | boolean | 否    | true                |
| date_format                 | string  | 否    | yyyy-MM-dd          |
| datetime_format             | string  | 否    | yyyy-MM-dd HH:mm:ss |
| time_format                 | string  | 否    | HH:mm:ss            |
| skip_header_row_number      | long    | 否    | 0                   |
| schema                      | config  | 否    | -                   |
| sheet_name                  | string  | 否    | -                   |
| xml_row_tag                 | string  | 否    | -                   |
| xml_use_attr_format         | boolean | 否    | -                   |
| csv_use_header_line         | boolean | 否    | false               |
| file_filter_pattern         | string  | 否    | -                   |
| compress_codec              | string  | 否    | none                |
| archive_compress_codec      | string  | 否    | none                |
| encoding                    | string  | 否    | UTF-8               |
| null_format                 | string  | 否    | -                   |
| binary_chunk_size           | int     | 否    | 1024                |
| binary_complete_file_mode   | boolean | 否    | false               |
| common-options              |         | 否    | -                   |
| file_filter_modified_start  | string  | 否    | -                   | 
| file_filter_modified_end    | string  | 否    | -                   | 

### host [string]

目标 FTP 主机地址，必填项。

### port [int]

目标 FTP 端口，必填项。

### user [string]

目标 FTP 用户名，必填项。

### password [string]

目标 FTP 密码，必填项。

### path [string]

源文件路径。

### remote_verification_enabled [boolean]

是否启用FTP数据通道的远程主机验证。默认值为 `true`。

### file_filter_pattern [string]

文件过滤模式，用于过滤文件。

该模式遵循标准正则表达式。详情请参考：https://en.wikipedia.org/wiki/Regular_expression.
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

**示例 1**：*匹配所有 .txt 文件*，正则表达式：
```
/data/seatunnel/20241001/.*\.txt
```
该示例匹配结果为：
```
/data/seatunnel/20241001/report.txt
```
**示例 2**：*匹配所有以 abc 开头的文件*，正则表达式：
```
/data/seatunnel/20241002/abc.*
```
该示例匹配结果为：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**示例 3**：*匹配所有以 abc 开头的文件，且第四个字符为 h 或 g*，正则表达式：
```
/data/seatunnel/20241007/abc[h,g].*
```
该示例匹配结果为：
```
/data/seatunnel/20241007/abch202410.csv
```
**示例 4**：*匹配第三级文件夹以 202410 开头且文件以 .csv 结尾的文件*，正则表达式：
```
/data/seatunnel/202410\d*/.*\.csv
```
该示例匹配结果为：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### file_format_type [string]

文件类型，支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`

如果您将文件类型指定为 `json`，您还需要指定 schema 选项以告诉连接器如何将数据解析为您所需的行。

例如：

上游数据如下：

```json

{"code":  200, "data":  "get success", "success":  true}

```

您应按如下方式指定 schema：

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

如果您将文件类型指定为 `text` 或 `csv`，您可以选择是否指定 schema 信息。

例如，上游数据如下：

```text

tyrantlucifer#26#male

```

如果您不指定数据 schema，连接器将按如下方式处理上游数据：

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

如果您指定数据 schema，您还需要指定 `field_delimiter` 选项（CSV 文件类型除外）。

您应按如下方式指定 schema 和分隔符：

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

如果您将文件类型指定为 `binary`，SeaTunnel 可以同步任何格式的文件，
例如压缩包、图片等。简而言之，任何文件都可以同步到目标位置。
在这种情况下，您需要确保源和接收端同时使用 `binary` 格式进行文件同步。
您可以在下面的示例中找到具体用法。

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

### connection_mode [string]

目标 FTP 连接模式，默认为主动模式，支持以下模式：

`active_local` `passive_local`

### control_encoding [string]

FTP 控制连接的字符编码。默认为 `UTF-8`。

当文件路径包含特殊字符（如 `$`、空格、中文字符等）时，需要设置为 `UTF-8` 以确保路径能够正确解析。

例如：`/data/whale_ops/share/$Fund-Product/DA - SANY （三一）/Daily/2025.08.18/file.xlsx`

### delimiter/field_delimiter [string]

**delimiter** 参数将在 2.3.5 版本后弃用，请使用 **field_delimiter** 代替。

仅在文件格式为 text 时需要配置。

字段分隔符，用于告诉连接器如何切分字段。

默认值为 `\001`，与 Hive 的默认分隔符相同。

### parse_partition_from_path [boolean]

控制是否从文件路径中解析分区键和值。

例如，如果您从路径 `ftp://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26` 读取文件，

文件中的每条记录数据将添加以下两个字段：

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

提示：**不要在 schema 选项中定义分区字段**

### date_format [string]

日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式：

`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`

默认值为 `yyyy-MM-dd`

### datetime_format [string]

日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式：

`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`

默认值为 `yyyy-MM-dd HH:mm:ss`

### time_format [string]

时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式：

`HH:mm:ss` `HH:mm:ss.SSS`

默认值为 `HH:mm:ss`

### skip_header_row_number [long]

跳过前几行，仅适用于 txt 和 csv 文件。

例如，设置如下：

`skip_header_row_number = 2`

SeaTunnel 将从源文件中跳过前 2 行。

### schema [config]

仅在文件格式类型为 text、json、excel、xml 或 csv（或其他无法从元数据中读取 schema 的格式）时需要配置。

上游数据的 schema 信息。

### read_columns [list]

数据源的读取列列表，用户可以使用它来实现字段投影。

### sheet_name [string]

读取工作簿中的工作表，仅在文件格式类型为 excel 时使用。

### xml_row_tag [string]

仅在文件格式为 xml 时需要配置。

指定 XML 文件中数据行的标签名称。

### xml_use_attr_format [boolean]

仅在文件格式为 xml 时需要配置。

指定是否使用标签属性格式处理数据。

### csv_use_header_line [boolean]

仅在文件格式为 csv 时可以选择配置。
是否使用标题行来解析文件, 标题行 与 RFC 4180 匹配        

### compress_codec [string]

文件的压缩编解码器，支持的详细信息如下：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  自动识别压缩类型，无需额外设置。

### archive_compress_codec [string]

归档文件的压缩编解码器，支持的详细信息如下：

| archive_compress_codec | 文件格式        | 归档压缩后缀 |
|------------------------|--------------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

注意：gz 压缩的 excel 文件需要压缩原始文件或指定文件后缀，例如 e2e.xls ->e2e_test.xls.gz

### encoding [string]

仅在文件格式类型为 json、text、csv、xml 时使用。
读取文件的编码。此参数将通过 `Charset.forName(encoding)` 解析。

### null_format [string]

仅在文件格式类型为 text 时使用。
用于定义哪些字符串可以表示为 null。

例如：`\N`

### binary_chunk_size [int]

仅在 file_format_type 为 binary 时使用。

读取二进制文件的块大小（以字节为单位）。默认为 1024 字节。较大的值可能会提高大文件的性能，但会使用更多内存。

### binary_complete_file_mode [boolean]

仅在 file_format_type 为 binary 时使用。

是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为 false。

### file_filter_modified_start

按照最后修改时间过滤文件。 要过滤的开始时间(包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`。

### file_filter_modified_end

按照最后修改时间过滤文件。 要过滤的结束时间(不包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`。

### 通用选项

源插件的通用参数，详情请参考 [源通用选项](../source-common-options.md)。

## 示例

```hocon

  FtpFile {
    path = "/tmp/seatunnel/sink/text"
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    file_format_type = "text"
    schema = {
      name = string
      age = int
    }
    field_delimiter = "#"
  }

```

### 多表配置

```hocon

FtpFile {
  tables_configs = [
    {
      schema {
        table = "student"
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

```hocon

FtpFile {
  tables_configs = [
    {
      schema {
        fields {
          name = string
          age = int
        }
      }
      path = "/apps/hive/demo/student"
      file_format_type = "json"
    },
    {
      schema {
        fields {
          name = string
          age = int
        }
      }
      path = "/apps/hive/demo/teacher"
      file_format_type = "json"
    }
}

```

### 传输二进制文件

```hocon

env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FtpFile {
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
    binary_chunk_size = 2048
    binary_complete_file_mode = false
  }
}
sink {
  // 您可以将本地文件传输到 s3/hdfs/oss 等。
  FtpFile {
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    path = "/seatunnel/read/binary2/"
    file_format_type = "binary"
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
  FtpFile {
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
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