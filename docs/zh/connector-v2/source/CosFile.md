import ChangeLog from '../changelog/connector-file-cos.md';

# CosFile

> CosFile source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)

在pollNext调用中读取拆分的所有数据。读取的拆分内容将保存在快照中。

- [x] [列映射](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义拆分](../../concept/connector-v2-features.md)
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

从阿里云Cos文件系统读取数据。

:::提示

如果你使用spark/flink，为了使用这个连接器，你必须确保你的spark/flilk集群已经集成了hadoop。测试的hadoop版本是2.x

如果你使用SeaTunnel Engine，当你下载并安装SeaTunnel引擎时，它会自动集成hadoop jar。您可以在${SEATUNNEL_HOME}/lib下检查jar包以确认这一点.

要使用此连接器，您需要将hadoop-cos-{hadoop.version}-{version}.jar和cos_api-bundle-{version}.jar位于${SEATUNNEL_HOME}/lib目录中，下载：[Hadoop-Cos-release](https://github.com/tencentyun/hadoop-cos/releases). 它只支持hadoop 2.6.5+和8.0.2版本+.

:::

## 选项

|名称                        |  类型    | 必需    | 默认值                |
|---------------------------|---------|---------|---------------------|
| path                      | string  | 是      | -                   |
| file_format_type          | string  | 是      | -                   |
| bucket                    | string  | 是      | -                   |
| secret_id                 | string  | 是      | -                   |
| secret_key                | string  | 是      | -                   |
| region                    | string  | 是      | -                   |
| read_columns              | list    | 是      | -                   |
| delimiter/field_delimiter | string  | 否       | \001                |
| parse_partition_from_path | boolean | 否       | true                |
| skip_header_row_number    | long    | 否       | 0                   |
| date_format               | string  | 否       | yyyy-MM-dd          |
| datetime_format           | string  | 否       | yyyy-MM-dd HH:mm:ss |
| time_format               | string  | 否       | HH:mm:ss            |
| schema                    | config  | 否       | -                   |
| sheet_name                | string  | 否       | -                   |
| xml_row_tag               | string  | 否       | -                   |
| xml_use_attr_format       | boolean | 否       | -                   |
| file_filter_pattern       | string  | 否       |                     |
| compress_codec            | string  | 否       | none                |
| archive_compress_codec    | string  | 否       | none                |
| encoding                  | string  | 否       | UTF-8               |
| common-options            |         | 否       | -                   |

### path [string]

源文件路径。

### file_format_type [string]

文件类型，支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

如果您将文件类型设置为“json”，您还应该分配模式选项，告诉连接器如何将数据解析到所需的行。

例如:

上游数据如下:

```json

{"code":  200, "data":  "get success", "success":  true}

```

您还可以将多条数据保存在一个文件中，并按换行符拆分它们:

```json lines

{"code":  200, "data":  "get success", "success":  true}
{"code":  300, "data":  "get failed", "success":  false}

```

您应该按如下方式设置schema架构:

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

连接器将按如下方式生成数据:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

如果您将文件类型指定为“parquet” “orc”，则不需要模式选项，连接器可以自动找到上游数据的模式。

如果将文件类型指定为“text” “csv”，则可以选择是否指定schema架构信息。

例如，上游数据如下:

```text

tyrantlucifer#26#male

```

如果不指定数据schema模式，连接器将按如下方式处理上游数据:

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

如果指定数据模式，除了CSV文件类型外，还应指定“field_delimiter”选项

您应该按如下方式分配模式和分隔符:

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

连接器将按如下方式生成数据:

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

如果将文件类型指定为“二进制”，SeaTunnel可以同步任何格式的文件，
例如压缩包、图片等。简而言之，任何文件都可以同步到目标位置。
根据此要求，您需要确保源端和目标端使用“二进制”格式进行文件同步同时。您可以在下面的示例中找到具体用法。

### bucket [string]

Cos文件系统的bucket地址，例如: `cos://tyrantlucifer-image-bed`

### secret_id [string]

Cos文件系统的秘密id。

### secret_key [string]

Cos文件系统的密钥。

### region [string]

cos文件系统的region。

### read_columns [list]

读取数据源的列的列表，用户可以使用它来实现字段映射。

### delimiter/field_delimiter [string]

**delimiter** 参数在2.3.5版本后将弃用，请改用**field_delimiter**。

仅当file_format为文本时才需要配置。

字段分隔符，用于告诉连接器如何对字段进行切片和切块

默认值“\001”，与配置单元的默认分隔符相同

### parse_partition_from_path [boolean]

控制是否从文件路径解析分区键和值

例如，如果从路径读取文件`cosn://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

文件中的每个记录数据都将添加这两个字段:

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

提示：**不要在schema选项中定义分区字段**

### skip_header_row_number [long]

跳过前几行，但仅限于txt和csv。

例如，设置如下:

`skip_header_row_number = 2`

那么SeaTunnel将跳过源文件的前两行

### date_format [string]

日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式:

`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`

default `yyyy-MM-dd`

### datetime_format [string]

Datetime类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式:

`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`

default `yyyy-MM-dd HH:mm:ss`

### time_format [string]

时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式:

`HH:mm:ss` `HH:mm:ss.SSS`

default `HH:mm:ss`

### schema [config]

仅当file_format_type为文本、json、excel、xml或csv（或我们无法从元数据中读取模式的其他格式）时才需要配置。

#### fields [Config]

上游数据的模式。

### sheet_name [string]

仅当file_format为excel时才需要配置。

阅读工作簿的纸张。

### xml_row_tag [string]

仅当file_format为xml时才需要配置。

指定XML文件中数据行的标记名称。

### xml_use_attr_format [boolean]

仅当file_format为xml时才需要配置。
指定是否使用标记属性格式处理数据。

### file_filter_pattern [string]

过滤模式，用于过滤文件。

该模式遵循标准正则表达式。详情请参阅https://en.wikipedia.org/wiki/Regular_expression.
有一些例子。

文件结构示例:
```
/data/seatunnel/20241001/report.txt
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
/data/seatunnel/20241012/logo.png
```
匹配规则示例:

**示例1**：*匹配所有.txt文件*，正则表达式：
```
/data/seatunnel/20241001/.*\.txt
```
此示例匹配的结果为：
```
/data/seatunnel/20241001/report.txt
```
**示例2**:*匹配所有以abc*开头的文件，正则表达式：
```
/data/seatunnel/20241002/abc.*
```
此示例匹配的结果为：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**示例3**：*匹配所有以abc开头的文件，第四个字符是h或g*，正则表达式：
```
/data/seatunnel/20241007/abc[h,g].*
```
此示例匹配的结果为：
```
/data/seatunnel/20241007/abch202410.csv
```
**示例4**:*匹配以202410开头的三级文件夹和以.csv*结尾的文件，正则表达式：
```
/data/seatunnel/202410\d*/.*\.csv
```
此示例匹配的结果为：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### compress_codec [string]

文件的压缩编解码器和支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  自动识别压缩类型，无需额外设置。

### archive_compress_codec [string]

归档文件的压缩编解码器和支持的详细信息如下所示：

| archive_compress_codec | file_format        | archive_compress_suffix |
|------------------------|--------------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

注意：gz压缩的excel文件需要压缩原始文件或指定文件后缀，如e2e.xls->e2e_test.xls.gz

### encoding [string]

仅当file_format_type为json、text、csv、xml时使用。
要读取的文件的编码。此参数将由`Charset.forName（encoding）`解析。

### common options

源插件常用参数，详见[源端通用选项]（../Source-common-Options.md）。

## 例如

```hocon

  CosFile {
    path = "/seatunnel/orc"
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    file_format_type = "orc"
  }

```

```hocon

  CosFile {
    path = "/seatunnel/json"
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    file_format_type = "json"
    schema {
      fields {
        id = int 
        name = string
      }
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
  CosFile {
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
  }
}
sink {
  // 您可以将本地文件传输到s3/hdfs/oss等。
  CosFile {
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    path = "/seatunnel/read/binary2/"
    file_format_type = "binary"
  }
}

```

### Filter File

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  CosFile {
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
    // file example abcD2024.csv
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
