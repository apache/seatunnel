import ChangeLog from '../changelog/connector-file-cos.md';

# CosFile

> CosFile source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在pollNext调用中读取拆分的所有数据。读取的拆分内容将保存在快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)
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
  - [x] pdf

## 描述

从腾讯云 COS 文件系统读取数据。

:::提示

如果你使用 Spark/Flink，为了使用这个连接器，你必须确保 Spark/Flink 集群已经集成了 Hadoop。测试的 Hadoop 版本是 2.x。

如果你使用 SeaTunnel Engine，当你下载并安装 SeaTunnel Engine 时，它会自动集成 Hadoop jar。你可以在 `${SEATUNNEL_HOME}/lib` 下检查 jar 包以确认这一点。

要使用此连接器，你需要将 `hadoop-cos-{hadoop.version}-{version}.jar` 和 `cos_api-bundle-{version}.jar` 放在 `${SEATUNNEL_HOME}/lib` 目录中，下载地址：[Hadoop COS release](https://github.com/tencentyun/hadoop-cos/releases)。它仅支持 Hadoop 2.6.5+ 和 hadoop-cos 8.0.2+。

:::

## 选项

| 名称                         | 类型      | 必需 | 默认值                 |
|----------------------------|---------|----|---------------------|
| path                       | string  | 是  | -                   |
| file_format_type           | string  | 是  | -                   |
| bucket                     | string  | 是  | -                   |
| secret_id                  | string  | 是  | -                   |
| secret_key                 | string  | 是  | -                   |
| region                     | string  | 是  | -                   |
| read_columns               | list    | 否  | -                   |
| delimiter/field_delimiter  | string  | 否  | \001                |
| row_delimiter              | string  | 否  | \n                  |
| parse_partition_from_path  | boolean | 否  | true                |
| skip_header_row_number     | long    | 否  | 0                   |
| date_format                | string  | 否  | yyyy-MM-dd          |
| datetime_format            | string  | 否  | yyyy-MM-dd HH:mm:ss |
| time_format                | string  | 否  | HH:mm:ss            |
| schema                     | config  | 否  | -                   |
| sheet_name                 | string  | 否  | -                   |
| excel_engine               | string  | 否  | POI                |
| poi_excel_max_file_size    | long    | 否  | 52428800           |
| xml_row_tag                | string  | 否  | -                   |
| xml_use_attr_format        | boolean | 否  | -                   |
| csv_use_header_line        | boolean | 否  | false               |
| file_filter_pattern        | string  | 否  |                     |
| filename_extension         | string  | 否  | -                   | 使用指定的文件扩展名筛选文件，例如 `csv`、`.txt`、`json` 或 `.xml`。 |
| compress_codec             | string  | 否  | none                |
| archive_compress_codec     | string  | 否  | none                |
| encoding                   | string  | 否  | UTF-8               |
| binary_chunk_size          | int     | 否  | 1024                |
| binary_complete_file_mode  | boolean | 否  | false               |
| common-options             |         | 否  | -                   |
| file_filter_modified_start | string  | 否  | -                   |
| file_filter_modified_end   | string  | 否  | -                   |
| quote_char                 | string  | 否  | "                   | 
| escape_char                | string  | 否  | -                   |
| recursive_file_scan        | boolean | 否  | true                |
| sort_files_by_modification_time | boolean | 否 | false               | 是否按修改时间降序排序文件。启用此选项后，在读取不断演化的 schema 时可确保 schema 推断使用最新的文件。                                                                                                                      |

### path [string]

源文件路径。

### file_format_type [string]

文件类型，支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`

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

如果您将文件类型指定为 `markdown`，SeaTunnel 可以解析 markdown 文件并提取结构化数据。
markdown 解析器提取各种元素，包括标题、段落、列表、代码块、表格等。
每个提取出的元素都会转换为一条文档元素结构化记录，schema 如下：
- `element_id`：元素的唯一标识符
- `element_type`：元素类型（Heading、Paragraph、ListItem 等）
- `heading_level`：标题级别（1-6，非标题元素为 null）
- `text`：元素的文本内容
- `page_number`：页码（默认：1）
- `position_index`：文档中的位置索引
- `parent_id`：父元素的 ID
- `child_ids`：子元素 ID 的逗号分隔列表

当 `markdown_rag_metadata_enabled` 或 `pdf_rag_metadata_enabled` 设置为 `true` 时，SeaTunnel 会针对对应文件类型在 `child_ids` 之后追加以下 RAG 元数据字段：
- `source_uri`：源文件路径或 URI
- `document_id`：由 `source_uri` 派生的稳定文档标识符
- `chunk_id`：由文档标识、chunk 顺序和内容哈希派生的稳定 chunk 标识符
- `chunk_index`：解析后文档中的一基 chunk 顺序
- `content_hash`：已输出 `text` 值的 SHA-256 哈希

启用该选项并读取有界 Markdown 文件时，source enumerator 会使用相同的 `document_id` 哈希分配整文件 split，使同一文档派生的所有行留在同一个 source 路由 bucket 中。禁用该选项时，默认的轮询 split 分配行为保持不变。

该选项默认值为 `false`，因此只有显式启用后才会改变原始 Markdown schema。

当 `markdown_rag_metadata_enabled=true` 时，每个 Markdown 行还会在 row options 中携带四个 Knowledge Sync 逻辑元数据值，source 也会在 metadata schema 中声明相同 Key：

- `SourceUri`：不含凭据的逻辑来源路径或 URI
- `DocumentId`：`doc_` 加逻辑 `SourceUri` 的 UTF-8 字节的小写 SHA-256
- `DocumentHash`：UTF-8 解码前实际读取到的精确来源字节的小写 SHA-256
- `ChunkHash`：当前 Markdown 输出行 `text` 的 UTF-8 字节的小写 SHA-256（null 按空字符串处理）；其值等于物理 `content_hash`

本地路径和有效 `file:` URI 沿用现有的本地路径归一化。对于分层远程 URI，逻辑 `SourceUri` 保留 scheme、host、显式端口和 path，移除 user info、完整 query 和 fragment，并将 scheme 与 host 转为小写。仅通过 query 区分资源时，必须改用稳定且不敏感的 path。

五个物理 RAG 字段、现有计算公式和路由行为均保持不变。因此，对于带签名或凭据的远程 URI，逻辑与物理 `document_id` 可能不同。请通过 [Metadata transform](../../transforms/metadata.md) 将逻辑 `SourceUri` 和 `DocumentId` 投影到 `ks_source_uri`、`ks_document_id` 等不冲突的别名。

逻辑 `ChunkHash` 只描述 Markdown source 直接输出的当前行。如果下游 transform 修改文本或把一行展开为多个 chunk，则必须在 lifecycle sink 前重新计算最终 `ChunkHash`、`ChunkId` 和 `ChunkIndex`。该 bridge 不实现增量比较、writer affinity、过期 chunk 删除或 tombstone。

注意：Markdown 格式仅支持读取，不支持写入。

如果您将文件类型指定为 `pdf`，SeaTunnel 可以解析 PDF 文件并提取结构化的文档元素。
PDF 使用与上文相同的文档元素 schema。
对于 PDF 输入，启用 `pdf_rag_metadata_enabled` 即可追加上文所述的 RAG 元数据字段。

PDF 特有的解析行为如下：

- **有大纲**：提取 `heading`（标题）、`paragraph`（段落）、`image`（图片）和 `link`（链接）元素。标题从大纲结构中派生，元素按照文档的逻辑结构组织为父子层级关系。
- **无大纲**：仅提取 `paragraph`（段落）和 `image`（图片）元素，以扁平结构呈现，不包含层级关系。
- `element_type` 在 PDF 场景下可能为 `heading`、`paragraph`、`image` 或 `link`。

注意：仅支持单栏（从上到下）PDF 布局。不支持多栏布局（例如并排的双栏文档），可能会产生不正确的文本顺序。

根据此要求，您需要确保源端和目标端使用“二进制”格式进行文件同步同时。您可以在下面的示例中找到具体用法。

### bucket [string]

Cos文件系统的bucket地址，例如: `cos://tyrantlucifer-image-bed`

### secret_id [string]

Cos 文件系统的 SecretId。在 [腾讯云 CAM 控制台](https://console.cloud.tencent.com/cam/capi) 创建。生产环境建议为作业分配一个绑定细粒度策略（如 `QcloudCOSReadOnlyAccess`）的 CAM 角色，并通过 STS 颁发临时密钥，避免长期密钥出现在作业配置里。

### secret_key [string]

Cos 文件系统的 SecretKey，与 `secret_id` 成对使用。生产建议参考 `secret_id`，改用 STS 临时密钥。

### region [string]

Cos 文件系统所在 region。请填入与 bucket 实际所在地域一致的 region（如 `ap-guangzhou`、`ap-shanghai`、`ap-chengdu`）。跨 region 访问虽然可行，但会产生跨地域传输费用和时延。

### read_columns [list]

读取数据源的列的列表，用户可以使用它来实现字段映射。

### delimiter/field_delimiter [string]

**delimiter** 参数在2.3.5版本后将弃用，请改用**field_delimiter**。

仅当file_format为文本时才需要配置。

字段分隔符，用于告诉连接器如何对字段进行切片和切块

默认值“\001”，与配置单元的默认分隔符相同

### row_delimiter [string]

仅在 file_format 为 text 时需要配置。

行分隔符，用于告诉连接器如何分割行。

默认 `\n`。

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

上游数据的schema。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### sheet_name [string]

仅当file_format为excel时才需要配置。

阅读工作簿的纸张。

### excel_engine [string]

仅在 `file_format` 为 excel 时使用。

支持的引擎包括 `POI` 和 `EasyExcel`。默认值为 `POI`。

默认的 Excel 读取引擎是 POI。POI 会保留历史读取行为，包括 POI 特有的公式和格式处理能力，但读取大 Excel 文件时可能占用大量内存。

如果需要读取大 Excel 文件，可以设置 `excel_engine = EasyExcel` 使用流式读取。

### poi_excel_max_file_size [long]

仅在 `file_format` 为 excel 且 `excel_engine` 为 POI 时使用。

POI 引擎允许读取的最大 Excel 文件大小，单位为字节。默认值为 `52428800` 字节（50 MB）。当文件超过该限制时，连接器会提前失败，并提示使用 EasyExcel。

### xml_row_tag [string]

仅当file_format为xml时才需要配置。

指定XML文件中数据行的标记名称。

### xml_use_attr_format [boolean]

仅当file_format为xml时才需要配置。
指定是否使用标记属性格式处理数据。

:::caution

出于安全考虑(XXE 加固), 包含 `<!DOCTYPE ...>` 声明的 XML 文件(`file_format_type = xml`)——即使是仅定义内部实体、不引用外部资源的良性声明——现在会被拒绝并抛出 `FILE_READ_FAILED` 错误。该行为没有配置项可以恢复为旧版本的处理方式。如果您的 XML 文件由某些工具导出并带有 `DOCTYPE` 头，请在使用 SeaTunnel 读取前将其移除或做预处理。

:::

### csv_use_header_line [boolean]

仅在文件格式为 csv 时可以选择配置。
是否使用标题行来解析文件, 标题行 与 RFC 4180 匹配

### file_filter_pattern [string]

文件过滤模式，用于过滤文件。若只想根据文件名称筛选，则直接写文件名称的正则；若同时想根据文件目录进行过滤，则表达式以`path`起始。

该模式遵循标准正则表达式。详情请参阅https://en.wikipedia.org/wiki/Regular_expression.
有一些例子。

若`path`为`/data/seatunnel`,且文件结构示例：
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
.*.txt
```
此示例匹配的结果为：
```
/data/seatunnel/20241001/report.txt
```
**示例2**:*匹配所有以abc*开头的文件，正则表达式：
```
abc.*
```
此示例匹配的结果为：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**示例3**：*匹配20241007文件夹下所有以 abc 开头的文件，且第四个字符为 h 或 g*，正则表达式：
```
/data/seatunnel/20241007/abc[h,g].*
```
此示例匹配的结果为：
```
/data/seatunnel/20241007/abch202410.csv
```
**示例4**:*匹配以202410开头的三级文件夹和以.csv*结尾的文件，正则表达式：
```
/data/seatunnel/202410\d*/.*.csv
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

### quote_char [string]

用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。

### escape_char [string]

用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。

### recursive_file_scan [boolean]

是否递归扫描子目录。
如果设置为 `false`，将忽略子目录，仅扫描指定路径下的文件。

### sort_files_by_modification_time [boolean]

是否按修改时间降序排序文件。默认值为 `false`。
启用后，文件将按修改时间排序（最新的在前）。适用于以下场景：
- 读取具有不断演化的 schema 的文件，且希望 schema 推断使用最新的文件
- 需要按时间顺序处理文件

### common options

源插件常用参数，详见[源端通用选项](../common-options/source-common-options.md)。

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
    binary_chunk_size = 2048
    binary_complete_file_mode = false
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
