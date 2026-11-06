import ChangeLog from '../changelog/connector-file-oss-jindo.md';

# OssJindoFile

> OssJindo 文件源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读写任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在一次 pollNext 调用中读取分割中的所有数据。读取哪些分割将保存在快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)
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

使用 Jindo API 从阿里云 OSS 文件系统读取数据。

:::tip

您需要下载 [jindosdk-4.6.1.tar.gz](https://jindodata-binary.oss-cn-shanghai.aliyuncs.com/release/4.6.1/jindosdk-4.6.1.tar.gz)
然后解压缩，从 lib 中复制 jindo-sdk-4.6.1.jar 和 jindo-core-4.6.1.jar 到 ${SEATUNNEL_HOME}/lib。

如果您使用 spark/flink，为了使用此连接器，您必须确保您的 spark/flink 集群已集成 hadoop。测试的 hadoop 版本是 2.x。

如果您使用 SeaTunnel 引擎，它会在您下载和安装 SeaTunnel 引擎时自动集成 hadoop jar。您可以检查 ${SEATUNNEL_HOME}/lib 下的 jar 包来确认这一点。

我们为了支持更多文件类型做了一些权衡，所以我们使用 HDFS 协议来内部访问 OSS，此连接器需要一些 hadoop 依赖项。
它仅支持 hadoop 版本 **2.9.X+**。

:::

## 选项

| 参数名                       | 类型      | 必须 | 默认值                         | 描述                                                                            |
|---------------------------|---------|----|-----------------------------|-------------------------------------------------------------------------------|
| path                      | string  | 是  | -                           | 目标目录路径                                                                        |
| file_format_type          | string  | 是  | -                           | 文件类型，支持以下文件类型：`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`                                                                          |
| bucket                    | string  | 是  | -                           | OSS 文件系统的桶地址                                                                  |
| access_key                | string  | 是  | -                           | OSS 文件系统的访问密钥                                                                 |
| access_secret             | string  | 是  | -                           | OSS 文件系统的访问密钥                                                                 |
| endpoint                  | string  | 是  | -                           | OSS 文件系统的端点                                                                   |
| read_columns              | list    | 否  | -                           | 数据源的读取列列表                                                                     |
| read_partitions | list | 否 | - | 用户希望读取的分区列表，例如 `["year=2024"]`。设置后仅读取这些分区。 |
| delimiter/field_delimiter | string  | 否  | \001                        | 字段分隔符                                                                         |
| row_delimiter             | string  | 否  | \n                          | 行分隔符                                                                          |
| parse_partition_from_path | boolean | 否  | true                        | 控制是否从文件路径解析分区键和值                                                              |
| date_format               | string  | 否  | yyyy-MM-dd                  | 日期类型格式                                                                        |
| datetime_format           | string  | 否  | yyyy-MM-dd HH:mm:ss         | 日期时间类型格式                                                                      |
| time_format               | string  | 否  | HH:mm:ss                    | 时间类型格式                                                                        |
| skip_header_row_number    | long    | 否  | 0                           | 跳过前几行                                                                         |
| schema                    | config  | 条件必填 | -                        | 上游数据的模式信息。当 `file_format_type` 为 `text`、`json`、`excel`、`csv` 或 `xml` 时必填；其他格式（如 `parquet`、`orc`）连接器可自动推断 schema。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| sheet_name                | string  | 否  | -                           | Excel 工作表名称                                                                   |
| excel_engine              | string  | 否  | POI                         | 仅在 `file_format` 为 excel 时使用。支持的引擎包括 `POI` 和 `EasyExcel`。                                                                                                                                            |
| poi_excel_max_file_size   | long    | 否  | 52428800                    | 仅在 `file_format` 为 excel 且 `excel_engine` 为 POI 时使用。POI 引擎允许读取的最大 Excel 文件大小（默认 50 MB）。                                                                                                                                            |
| xml_row_tag               | string  | 否  | -                           | XML 行标签                                                                       |
| xml_use_attr_format       | boolean | 否  | -                           | 是否使用 XML 属性格式                                                                 |
| csv_use_header_line       | boolean | 否  | false                       | 是否使用 CSV 标题行                                                                  |
| file_filter_pattern       | string  | 否  | -                           | 文件过滤模式                                                                        |
| filename_extension        | string  | 否  | -                           | 使用指定的文件扩展名筛选文件，例如 `csv`、`.txt`、`json` 或 `.xml`。 |
| compress_codec            | string  | 否  | none                        | 读取文件时使用的压缩格式。 |
| archive_compress_codec    | string  | 否  | none                        | 读取归档文件时使用的归档压缩格式。 |
| encoding                  | string  | 否  | UTF-8                       | 读取 `json`、`text`、`csv` 或 `xml` 文件时使用的字符编码。 |
| null_format               | string  | 否  | -                           | 当 `file_format_type` 为 `text` 或 `csv` 时使用。指定表示 null 的字段值。 对于配置了 `schema.columns[].defaultValue` 的 CSV，如果字段值匹配 `null_format`，或文件表头缺少该列，则使用配置的默认值。空 CSV 字段仍为 null，除非它与 `null_format` 匹配；未配置默认值的列保持原有行为。 CSV 默认值在创建读取器时转换为列类型。无法转换的默认值会导致作业失败，即使没有字段需要使用该默认值。DATE 和 TIMESTAMP 默认值可以使用与文件数据不同的受支持格式。 例如：`\N` |
| binary_chunk_size         | int     | 否  | 1024                        | 仅在 file_format_type 为 binary 时使用。读取二进制文件的块大小（以字节为单位）。默认为 1024 字节。较大的值可能会提高大文件的性能，但会使用更多内存。 |
| binary_complete_file_mode | boolean | 否  | false                       | 仅在 file_format_type 为 binary 时使用。是否将完整文件作为单个块读取，而不是分割成块。 |
| common-options            |         | 否  | -                           | Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。 |
| file_filter_modified_start | string | 否  | -                           | 按文件最后修改时间筛选文件的起始时间（包含该时间）。 |
| file_filter_modified_end  | string  | 否  | -                           | 按文件最后修改时间筛选文件的结束时间（不包含该时间）。 |
| quote_char                | string  | 否  | "                           | 用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。                                       |
| escape_char               | string  | 否  | -                           | 用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。                                              |
| recursive_file_scan       | boolean | 否  | true                        | 是否递归扫描子目录。 如果设置为 `false`，将忽略子目录，仅扫描指定路径下的文件。                                  | 
| sort_files_by_modification_time | boolean | 否 | false               | 是否按修改时间降序排序文件。启用此选项后，在读取不断演化的 schema 时可确保 schema 推断使用最新的文件。                                                                                                                      |

### path [string]

源文件路径。

### file_format_type [string]

文件类型，支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`

如果您将文件类型指定为 `json`，您还应该指定 schema 选项来告诉连接器如何将数据解析为您想要的行。

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

您应该按如下方式指定 schema：

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

如果您将文件类型指定为 `parquet` `orc`，则不需要 schema 选项，连接器可以自动找到上游数据的 schema。

如果您将文件类型指定为 `text` `csv`，则必须指定 schema 信息：本连接器对 `text`、`json`、`excel`、`csv`、`xml` 格式均要求配置 `schema`，缺失时作业初始化会失败。

例如，上游数据如下：

```text

tyrantlucifer#26#male

```

如果希望将整行保留为单列，请显式配置如下 `schema`：

```hocon

schema {
    fields {
        content = string
    }
}

```

连接器将生成如下数据：

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

您也可以将每行拆分为多列，此时需要配置与字段匹配的 `schema`，并（除 CSV 文件类型外）同时指定 `field_delimiter` 选项，配置方式如下：

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

如果您将文件类型指定为 `binary`，SeaTunnel 可以同步任意格式的文件，
例如压缩包、图片等。简而言之，任何文件都可以同步到目标位置。
在此需求下，您需要确保 source 和 sink 同时使用 `binary` 格式进行文件同步。
具体用法可以在下面的示例中找到。

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
- `child_ids`：直接子元素的 ID，类型为 `ARRAY<STRING>`；叶子元素为空数组

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

:::caution

出于安全考虑(XXE 加固), 包含 `<!DOCTYPE ...>` 声明的 XML 文件(`file_format_type = xml`)——即使是仅定义内部实体、不引用外部资源的良性声明——现在会被拒绝并抛出 `FILE_READ_FAILED` 错误。该行为没有配置项可以恢复为旧版本的处理方式。如果您的 XML 文件由某些工具导出并带有 `DOCTYPE` 头，请在使用 SeaTunnel 读取前将其移除或做预处理。

:::

### bucket [string]

OSS 文件系统的桶地址，例如：`oss://tyrantlucifer-image-bed`

### access_key [string]

OSS 文件系统的访问密钥。

### access_secret [string]

OSS 文件系统的访问密钥（密钥）。

### endpoint [string]

OSS 文件系统的端点。

### read_columns [list]

数据源的读取列列表，用户可以使用它来实现字段投影。

### delimiter/field_delimiter [string]

**delimiter** 参数将在 2.3.5 版本后废弃，请改用 **field_delimiter**。

仅在 file_format 为 text 时需要配置。

字段分隔符，用于告诉连接器如何切分字段。

默认 `\001`，与 hive 的默认分隔符相同

### row_delimiter [string]

仅在 file_format 为 text 时需要配置。

行分隔符，用于告诉连接器如何切分行。

默认 `\n`

### parse_partition_from_path [boolean]

控制是否从文件路径解析分区键和值。

例如，如果您从路径 `oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26` 读取文件。

文件中的每条记录数据都会添加这两个字段：

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

提示：**不要在 schema 选项中定义分区字段**

### date_format [string]

日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式：

`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`

默认 `yyyy-MM-dd`

### datetime_format [string]

日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式：

`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`

默认 `yyyy-MM-dd HH:mm:ss`

### time_format [string]

时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式：

`HH:mm:ss` `HH:mm:ss.SSS`

默认 `HH:mm:ss`

### skip_header_row_number [long]

跳过前几行，但仅适用于 txt 和 csv。

例如，设置如下：

`skip_header_row_number = 2`

然后 SeaTunnel 将跳过源文件的前 2 行。

### schema [config]

仅在 file_format_type 为 text、json、excel、xml 或 csv 时需要配置（或其他我们无法从元数据读取 schema 的格式）。

#### fields [Config]

上游数据的 schema。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### sheet_name [string]

仅在 file_format 为 excel 时需要配置。

读取工作簿的工作表。

### excel_engine [string]

仅在 `file_format` 为 excel 时使用。

支持的引擎为 `POI` 和 `EasyExcel`。默认值为 `POI`。

默认的 Excel 读取引擎是 POI。POI 保持了历史读取行为，包括 POI 特有的公式和格式处理，但对于较大的 Excel 文件可能会占用大量内存。

您可以设置 `excel_engine = EasyExcel` 以使用流式读取来处理大型 Excel 文件。

### poi_excel_max_file_size [long]

仅在 `file_format` 为 excel 且 `excel_engine` 为 POI 时使用。

POI 引擎可以读取的最大 Excel 文件大小（以字节为单位）。默认值为 `52428800` 字节（50 MB）。当文件超过此限制时，连接器会快速失败并建议使用 EasyExcel。

### file_filter_pattern [string]

文件过滤模式，用于过滤文件。若只想根据文件名称筛选，则直接写文件名称的正则；若同时想根据文件目录进行过滤，则表达式以 `path` 起始。

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
.*.txt
```
此示例匹配的结果是：
```
/data/seatunnel/20241001/report.txt
```
**示例2**：*匹配所有以abc开头的文件*，正则表达式：
```
abc.*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**示例3**：*匹配20241007文件夹下所有以 abc 开头的文件，且第四个字符为 h 或 g*，正则表达式：
```
/data/seatunnel/20241007/abc[h,g].*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
```
**示例4**：*匹配以202410开头的第三级文件夹和以.csv结尾的文件*，正则表达式：
```
/data/seatunnel/202410\d*/.*.csv
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### filename_extension [string]

过滤文件名扩展名，用于过滤具有特定扩展名的文件。例如：`csv` `.txt` `json` `.xml`。

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
|------------------------|--------------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

注意：gz 压缩的 excel 文件需要压缩原始文件或指定文件后缀，例如 e2e.xls ->e2e_test.xls.gz

### encoding [string]

仅在 file_format_type 为 json、text、csv、xml 时使用。
要读取的文件的编码。此参数将由 `Charset.forName(encoding)` 解析。

### null_format [string]

当 `file_format_type` 为 `text` 或 `csv` 时使用。定义哪个字段值表示 null。
对于配置了 `schema.columns[].defaultValue` 的 CSV，如果字段值匹配 `null_format`，或文件表头缺少该列，则使用配置的默认值。空 CSV 字段仍为 null，除非它与 `null_format` 匹配；未配置默认值的列保持原有行为。
CSV 默认值在创建读取器时转换为列类型。无法转换的默认值会导致作业失败，即使没有字段需要使用该默认值。DATE 和 TIMESTAMP 默认值可以使用与文件数据不同的受支持格式。

例如：`\N`

### file_filter_modified_start [string]

文件修改时间过滤器。连接器将根据最后修改开始时间（包含开始时间）过滤部分文件。默认时间格式为 `yyyy-MM-dd HH:mm:ss`。

### file_filter_modified_end [string]

文件修改时间过滤器。连接器将根据最后修改结束时间（不包含结束时间）过滤部分文件。默认时间格式为 `yyyy-MM-dd HH:mm:ss`。

### quote_char [string]

用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。

### escape_char [string]

用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。

### recursive_file_scan [boolean]

是否递归扫描子目录。
如果设置为 `false`，将忽略子目录。

### sort_files_by_modification_time [boolean]

是否按修改时间降序排序文件。默认值为 `false`。
启用后，文件将按修改时间排序（最新的在前）。适用于以下场景：
- 读取具有不断演化的 schema 的文件，且希望 schema 推断使用最新的文件
- 需要按时间顺序处理文件

### common options

Source 插件通用参数，请参考 [Source Common Options](../common-options/source-common-options.md) 了解详情。

## 示例

```hocon

OssJindoFile {
    path = "/seatunnel/orc"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
  }

```

```hocon

OssJindoFile {
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

```

### 传输二进制文件

```hocon

env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OssJindoFile {
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
  }
}
sink {
  // you can transfer local file to s3/hdfs/oss etc.
  OssJindoFile {
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
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
  OssJindoFile {
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
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
