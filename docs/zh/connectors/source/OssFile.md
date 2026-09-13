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

1. 您必须确保`seatunnel-shade-hadoop3-uber-3.1.4-3.0.0.jar`、`aliyun-sdk-oss-3.4.1.jar`、`hadoop-aliyun-3.1.4.jar`和`jdom-1.1.jar`在`${SEATUNNEL_HOME}/lib/`目录中。

## 主要特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在一次pollNext调用中读取分片中的所有数据。将读取的分片保存在快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义的分片](../../introduction/concepts/connector-v2-features.md)
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

| Orc数据类型                          | SeaTunnel数据类型                 |
|----------------------------------|-------------------------------|
| BOOLEAN                          | BOOLEAN                       |
| INT                              | INT                           |
| BYTE                             | BYTE                          |
| SHORT                            | SHORT                         |
| LONG                             | LONG                          |
| FLOAT                            | FLOAT                         |
| DOUBLE                           | DOUBLE                        |
| BINARY                           | BINARY                        |
| STRING<br/>VARCHAR<br/>CHAR<br/> | STRING                        |
| DATE                             | LOCAL_DATE_TYPE               |
| TIMESTAMP                        | LOCAL_DATE_TIME_TYPE          |
| DECIMAL                          | DECIMAL                       |
| LIST(STRING)                     | STRING_ARRAY_TYPE             |
| LIST(BOOLEAN)                    | BOOLEAN_ARRAY_TYPE            |
| LIST(TINYINT)                    | BYTE_ARRAY_TYPE               |
| LIST(SMALLINT)                   | SHORT_ARRAY_TYPE              |
| LIST(INT)                        | INT_ARRAY_TYPE                |
| LIST(BIGINT)                     | LONG_ARRAY_TYPE               |
| LIST(FLOAT)                      | FLOAT_ARRAY_TYPE              |
| LIST(DOUBLE)                     | DOUBLE_ARRAY_TYPE             |
| Map<K,V>                         | MapType，K和V的类型将转换为SeaTunnel类型 |
| STRUCT                           | SeaTunnelRowType              |

### Parquet文件类型

如果您将文件类型指定为`parquet` `orc`，则不需要schema选项，连接器可以自动找到上游数据的schema。

| Parquet数据类型          | SeaTunnel数据类型                 |
|----------------------|-------------------------------|
| INT_8                | BYTE                          |
| INT_16               | SHORT                         |
| DATE                 | DATE                          |
| TIMESTAMP_MILLIS     | TIMESTAMP                     |
| INT64                | LONG                          |
| INT96                | TIMESTAMP                     |
| BINARY               | BYTES                         |
| FLOAT                | FLOAT                         |
| DOUBLE               | DOUBLE                        |
| BOOLEAN              | BOOLEAN                       |
| FIXED_LEN_BYTE_ARRAY | TIMESTAMP<br/> DECIMAL        |
| DECIMAL              | DECIMAL                       |
| LIST(STRING)         | STRING_ARRAY_TYPE             |
| LIST(BOOLEAN)        | BOOLEAN_ARRAY_TYPE            |
| LIST(TINYINT)        | BYTE_ARRAY_TYPE               |
| LIST(SMALLINT)       | SHORT_ARRAY_TYPE              |
| LIST(INT)            | INT_ARRAY_TYPE                |
| LIST(BIGINT)         | LONG_ARRAY_TYPE               |
| LIST(FLOAT)          | FLOAT_ARRAY_TYPE              |
| LIST(DOUBLE)         | DOUBLE_ARRAY_TYPE             |
| Map<K,V>             | MapType，K和V的类型将转换为SeaTunnel类型 |
| STRUCT               | SeaTunnelRowType              |

## 选项

| 名称                         | 类型      | 是否必需 | 默认值                | 描述                                                                                                                                                   |
|----------------------------|---------|------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                       | string  | 是    | -                  | 需要读取的Oss路径，可以有子路径，但子路径需要满足一定的格式要求。具体要求可以参考"parse_partition_from_path"选项                                                                              |
| file_format_type           | string  | 是    | -                  | 文件类型，支持以下文件类型：`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`                                                                  |
| bucket                     | string  | 是    | -                  | oss文件系统的bucket地址，例如：`oss://seatunnel-test`。                                                                                                          |
| endpoint                   | string  | 是    | -                  | fs oss端点                                                                                                                                             |
| read_columns               | list    | 否    | -                  | 数据源的读取列列表，用户可以使用它来实现字段投影。支持列投影的文件类型如下所示：`text` `csv` `parquet` `orc` `json` `excel` `xml`。如果用户想在读取`text` `json` `csv`文件时使用此功能，必须配置"schema"选项。        |
| access_key                 | string  | 否    | -                  |                                                                                                                                                      |
| access_secret              | string  | 否    | -                  |                                                                                                                                                      |
| delimiter                  | string  | 否    | \001               | 字段分隔符，用于告诉连接器在读取文本文件时如何切分字段。默认`\001`，与hive的默认分隔符相同。                                                                                                  |
| row_delimiter              | string  | 否    | \n                 | 行分隔符，用于告诉连接器在读取文本文件时如何切分行。默认`\n`。                                                                                                                    |
| parse_partition_from_path  | boolean | 否    | true               | 控制是否从文件路径解析分区键和值。例如，如果您从路径`oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`读取文件。文件中的每条记录数据都将添加这两个字段：name="tyrantlucifer"，age=16 |
| date_format                | string  | 否    | yyyy-MM-dd         | 日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式：`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`。默认`yyyy-MM-dd`                                                               |
| datetime_format            | string  | 否    | yyyy-MM-dd HH:mm:ss | 日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式：`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`                              |
| time_format                | string  | 否    | HH:mm:ss           | 时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式：`HH:mm:ss` `HH:mm:ss.SSS`                                                                                           |
| filename_extension         | string  | 否    | -                  | 过滤文件名扩展名，用于过滤具有特定扩展名的文件。例如：`csv` `.txt` `json` `.xml`。                                                                                               |
| skip_header_row_number     | long    | 否    | 0                  | 跳过前几行，但仅适用于txt和csv。例如，设置如下：`skip_header_row_number = 2`。然后SeaTunnel将跳过源文件的前2行                                                                        |
| csv_use_header_line        | boolean | 否    | false              | 是否使用标题行来解析文件，仅在file_format为`csv`且文件包含符合RFC 4180的标题行时使用                                                                                               |
| schema                     | config  | 否    | -                  | 上游数据的schema。                                                                                                                                         |
| sheet_name                 | string  | 否    | -                  | 读取工作簿的工作表，仅在file_format为excel时使用。                                                                                                                    |
| excel_engine               | string  | 否    | POI                | 仅在 `file_format` 为 excel 时使用。支持的引擎包括 `POI` 和 `EasyExcel`。                                                                                                                                                                 |
| poi_excel_max_file_size    | long    | 否    | 52428800           | 仅在 `file_format` 为 excel 且 `excel_engine` 为 POI 时使用。POI 引擎允许读取的最大 Excel 文件大小（默认 50 MB）。                                                                                                                                                                 |
| xml_row_tag                | string  | 否    | -                  | 指定XML文件中数据行的标签名称，仅在file_format为xml时使用。                                                                                                               |
| xml_use_attr_format        | boolean | 否    | -                  | 指定是否使用标签属性格式处理数据，仅在file_format为xml时使用。                                                                                                               |
| compress_codec             | string  | 否    | none               | 文件使用的压缩编解码器。                                                                                                                                         |
| encoding                   | string  | 否    | UTF-8              |
| null_format                | string  | 否    | -                  | 仅在file_format_type为text时使用。null_format用于定义哪些字符串可以表示为null。例如：`\N`                                                                                     |
| binary_chunk_size          | int     | 否    | 1024               | 仅在file_format_type为binary时使用。读取二进制文件的块大小（以字节为单位）。默认为1024字节。较大的值可能会提高大文件的性能，但会使用更多内存。                                                                 |
| binary_complete_file_mode  | boolean | 否    | false              | 仅在file_format_type为binary时使用。是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为false。                                                                     |
| discovery_mode             | string  | 否    | once               | 文件发现模式，支持 `once`（默认）和 `continuous`。continuous 模式会定期扫描路径，目前需要同时配置 `sync_mode=update` 和 `file_format_type=binary`。 |
| scan_interval              | string  | 否    | 10S                | `discovery_mode=continuous` 的轮询间隔，支持 `10S` 等简写和 `PT10S` 等 ISO-8601 格式。 |
| start_mode                 | string  | 否    | earliest           | 持续发现的初始扫描方式。`earliest` 会处理已有文件，`latest` 仅处理后续新增或变更。 |
| sync_mode                  | string  | 否    | full               | 文件同步模式。`update` 会将源对象与 `target_path` 对比，只读取新增或变更对象，目前仅支持 binary 格式。 |
| target_path                | string  | 否    | -                  | `sync_mode=update` 时必填，通常应与 sink 的 `path` 一致。 |
| target_hadoop_conf         | map     | 否    | -                  | `sync_mode=update` 时可选的目标文件系统 Hadoop 配置。 |
| update_strategy            | string  | 否    | distcp             | update 模式的对比策略，支持 `distcp` 和 `strict`。 |
| compare_mode               | string  | 否    | len_mtime          | update 模式的对比方式，支持 `len_mtime` 和 `checksum`；checksum 需要 `update_strategy=strict`。 |
| update_compare_parallelism | int     | 否    | 8                  | 目标对象元数据查询的最大并发数，有效范围为 `1-64`。 |
| update_compare_bulk_threshold | int  | 否    | 0                  | 同一目标父目录的候选数达到正数阈值时改用一次目录枚举；`0` 表示关闭自动批量枚举。 |
| post_sync_action           | string  | 否    | none               | 持续发现对象完成 checkpoint 后的可选动作，支持 `none`、`delete` 和 `backup`。 |
| backup_path                | string  | 否    | -                  | `post_sync_action=backup` 时必填，且备份路径不能与源 `path` 重叠。 |
| retention_max_age          | string  | 否    | -                  | `backup_path` 中 SeaTunnel 备份对象的可选最大保留时间。 |
| retention_check_interval   | string  | 否    | 1H                 | 配置备份保留策略时的清理扫描间隔。 |
| file_filter_pattern        | string  | 否    |                    | 过滤模式，用于过滤文件。                                                                                                                                         |
| common-options             | config  | 否    | -                  | 数据源插件通用参数，请参考[数据源通用选项](../common-options/source-common-options.md)了解详情。                                                                                             |
| file_filter_modified_start | string  | 否    | -                  | 按照最后修改时间过滤文件。 要过滤的开始时间(包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`                                                                                            |
| file_filter_modified_end   | string  | 否    | -                  | 按照最后修改时间过滤文件。 要过滤的结束时间(不包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`                                                                                           |
| quote_char                 | string  | 否    | "                   | 用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。                                                                                                              |
| escape_char                | string  | 否    | -                  | 用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。                                                                                                                     |
| metalake_type              | string  | 否    | gravitino         | Metalake 服务类型，目前支持 `gravitino`。                                                                                                                                            |
| recursive_file_scan        | boolean | 否    | true                | 是否递归扫描子目录。 如果设置为 `false`，将忽略子目录，仅扫描指定路径下的文件。                                                                                                         |
| sort_files_by_modification_time | boolean | 否 | false               | 是否按修改时间降序排序文件。启用此选项后，在读取不断演化的 schema 时可确保 schema 推断使用最新的文件。                                                                                                                      |

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

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`

:::caution

出于安全考虑(XXE 加固), 包含 `<!DOCTYPE ...>` 声明的 XML 文件(`file_format_type = xml`)——即使是仅定义内部实体、不引用外部资源的良性声明——现在会被拒绝并抛出 `FILE_READ_FAILED` 错误。该行为没有配置项可以恢复为旧版本的处理方式。如果您的 XML 文件由某些工具导出并带有 `DOCTYPE` 头，请在使用 SeaTunnel 读取前将其移除或做预处理。

:::

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

### file_filter_pattern [string]

文件过滤模式，用于过滤文件。若只想根据文件名称筛选，则直接写文件名称的正则；若同时想根据文件目录进行过滤，则表达式以`path`起始。

该模式遵循标准正则表达式。详情请参考 [正则表达式](https://en.wikipedia.org/wiki/Regular_expression)。
以下是一些示例。

若`path`为`/data/seatunnel`,且文件结构示例：
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

### schema [config]

仅在file_format_type为text、json、excel、xml或csv时需要配置（或其他我们无法从元数据读取schema的格式）。

#### fields [Config]

上游数据的schema。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

#### metadata_table_id [string]

元数据服务中的表标识符，用于获取表结构。对于 Gravitino，格式应为 `{catalog}.{database}.{table}`，例如 `mysql-catalog.test_db.users`。

当指定此参数时，连接器将从外部元数据服务获取表结构，而不是使用手动定义的 `columns`。

> 当使用 Gravitino 作为元数据源时，Gravitino 的列类型会自动转换为 SeaTunnel 数据类型。详细的类型映射信息请参考 [Gravitino 类型映射](../../introduction/concepts/gravitino-type-mapping.md)。

更多信息请参考 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。

## 持续发现

`discovery_mode=continuous` 会让流式作业保持运行，并定期轮询 OSS 中的新增或变更对象。该模式使用现有文件对比逻辑，不消费 OSS 事件通知，也不会为对象删除或覆盖生成 changelog 行。

持续发现目前需要同时配置 `file_format_type="binary"` 和 `sync_mode="update"`。`target_path` 应与 sink 的基础 `path` 一致，以便源端跳过未变化对象。默认的 `discovery_mode="once"` 会保持原有有界读取行为。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  OssFile {
    path = "/watch/source"
    bucket = "oss://seatunnel-test"
    endpoint = "oss-cn-hangzhou.aliyuncs.com"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    file_format_type = "binary"

    discovery_mode = "continuous"
    scan_interval = "10S"
    start_mode = "earliest"
    sync_mode = "update"
    target_path = "/watch/target"
  }
}

sink {
  OssFile {
    path = "/watch/target"
    tmp_path = "/watch/tmp"
    bucket = "oss://seatunnel-test"
    endpoint = "oss-cn-hangzhou.aliyuncs.com"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    file_format_type = "binary"
  }
}
```

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
