import ChangeLog from '../changelog/connector-file-local.md';

# LocalFile

> 本地文件数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在 pollNext 调用中读取分片中的所有数据。读取的分片将保存在快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义分片](../../introduction/concepts/connector-v2-features.md)
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

从本地文件系统读取数据。

:::tip

如果您使用 spark/flink，为了使用此连接器，您必须确保您的 spark/flink 集群已经集成了 hadoop。测试过的 hadoop 版本是 2.x。

如果您使用 SeaTunnel Engine，则在下载和安装 SeaTunnel Engine 时会自动集成 hadoop jar。您可以检查 `${SEATUNNEL_HOME}/lib` 下的 jar 包来确认这一点。

:::

## 选项

| 名称                         | 类型      | 是否必须 | 默认值                 |
|----------------------------|---------|------|---------------------|
| path                       | string  | 是    | -                   |
| file_format_type           | string  | 是    | -                   |
| read_columns               | list    | 否    | -                   |
| delimiter/field_delimiter  | string  | 否    | \001                |
| row_delimiter              | string  | 否    | \n                  |
| parse_partition_from_path  | boolean | 否    | true                |
| date_format                | string  | 否    | yyyy-MM-dd          |
| datetime_format            | string  | 否    | yyyy-MM-dd HH:mm:ss |
| time_format                | string  | 否    | HH:mm:ss            |
| skip_header_row_number     | long    | 否    | 0                   |
| schema                     | config  | 否    | -                   |
| sheet_name                 | string  | 否    | -                   |
| excel_engine               | string  | 否    | POI                 |
| poi_excel_max_file_size    | long    | 否    | 52428800            |
| xml_row_tag                | string  | 否    | -                   |
| xml_use_attr_format        | boolean | 否    | -                   |
| csv_use_header_line        | boolean | 否    | false               |
| file_filter_pattern        | string  | 否    | -                   |
| filename_extension         | string  | 否    | -                   |
| compress_codec             | string  | 否    | none                |
| archive_compress_codec     | string  | 否    | none                |
| encoding                   | string  | 否    | UTF-8               |
| null_format                | string  | 否    | -                   |
| binary_chunk_size          | int     | 否    | 1024                |
| binary_complete_file_mode  | boolean | 否    | false               |
| discovery_mode             | string  | 否    | once                |
| scan_interval              | string  | 否    | 10S |
| start_mode                 | string  | 否    | earliest            |
| sync_mode                  | string  | 否    | full                |
| target_path                | string  | 否    | -                   |
| target_hadoop_conf         | map     | 否    | -                   |
| update_strategy            | string  | 否    | distcp              |
| compare_mode               | string  | 否    | len_mtime           |
| update_compare_parallelism | int     | 否    | 8                   |
| update_compare_bulk_threshold | int  | 否    | 0                   |
| post_sync_action           | string  | 否    | none                |
| backup_path                | string  | 否    | -                   |
| retention_max_age          | string  | 否    | -                   |
| retention_check_interval   | string  | 否    | 1H                  |
| common-options             |         | 否    | -                   |
| tables_configs             | list    | 否    | 用于定义多表任务            |
| file_filter_modified_start | string  | 否    | -                   | 
| file_filter_modified_end   | string  | 否    | -                   |
| enable_file_split          | boolean | 否    | false               | 
| file_split_size            | long    | 否    | 134217728           | 
| quote_char                 | string  | 否    | "                   |
| escape_char                | string  | 否    | -                   |
| metalake_type              | string  | 否    | gravitino          | Metalake 服务类型，目前支持 `gravitino`。             |
| recursive_file_scan        | boolean | 否    | true                |
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

您也可以在一个文件中保存多条数据并用换行符分割：

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

如果您将文件类型指定为 `text` `csv`，您可以选择指定或不指定 schema 信息。

例如，上游数据如下：

```text

tyrantlucifer#26#male

```

如果您不指定数据 schema，连接器将把上游数据视为如下：

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

如果您指定数据 schema，除了 CSV 文件类型外，您还应该指定选项 `field_delimiter`

您应该按如下方式指定 schema 和分隔符：

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
在此要求下，您需要确保源和接收器同时使用 `binary` 格式进行文件同步。
您可以在下面的示例中找到具体用法。

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

### read_columns [list]

数据源的读取列列表，用户可以使用它来实现字段投影。

### delimiter/field_delimiter [string]

**delimiter** 参数将在 2.3.5 版本后弃用，请使用 **field_delimiter** 代替。

仅在 file_format 为 text 时需要配置。

字段分隔符，用于告诉连接器如何分割字段。

默认 `\001`，与 hive 的默认分隔符相同

### row_delimiter [string]

仅在 file_format 为 text 时需要配置。

行分隔符，用于告诉连接器如何分割行。

默认 `\n`。

### parse_partition_from_path [boolean]

控制是否从文件路径解析分区键和值

例如，如果您从路径 `file://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26` 读取文件

文件中的每条记录数据都将添加这两个字段：

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

然后 SeaTunnel 将跳过源文件的前 2 行

### schema [config]

仅在 file_format_type 为 text、json、excel、xml 或 csv（或其他我们无法从元数据读取 schema 的格式）时需要配置。

#### fields [Config]

上游数据的 schema 信息。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

#### metadata_table_id [string]

元数据服务中的表标识符，用于获取表结构。对于 Gravitino，格式应为 `{catalog}.{database}.{table}`，例如 `mysql-catalog.test_db.users`。

当指定此参数时，连接器将从外部元数据服务获取表结构，而不是使用手动定义的 `columns`。

> 当使用 Gravitino 作为元数据源时，Gravitino 的列类型会自动转换为 SeaTunnel 数据类型。详细的类型映射信息请参考 [Gravitino 类型映射](../../introduction/concepts/gravitino-type-mapping.md)。

更多信息请参考 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。

### sheet_name [string]

仅在 file_format 为 excel 时需要配置。

读取工作簿的工作表。

### excel_engine [string]

仅在 file_format 为 excel 时需要配置。

支持以下文件类型：
`POI` `EasyExcel`

默认的 Excel 读取引擎是 POI。POI 会保留历史读取行为，包括 POI 特有的公式和格式处理能力，但读取大 Excel 文件时可能占用大量内存。

如果需要读取大 Excel 文件，可以设置 `excel_engine = EasyExcel` 使用流式读取。

### poi_excel_max_file_size [long]

仅在 `file_format` 为 excel 且 `excel_engine` 为 POI 时使用。

POI 引擎允许读取的最大 Excel 文件大小，单位为字节。默认值为 `52428800` 字节（50 MB）。当文件超过该限制时，连接器会提前失败，并提示使用 EasyExcel。


### xml_row_tag [string]

仅在 file_format 为 xml 时需要配置。

指定 XML 文件中数据行的标签名称。

### xml_use_attr_format [boolean]

仅在 file_format 为 xml 时需要配置。

指定是否使用标签属性格式处理数据。

:::caution

出于安全考虑(XXE 加固), 包含 `<!DOCTYPE ...>` 声明的 XML 文件(`file_format_type = xml`)——即使是仅定义内部实体、不引用外部资源的良性声明——现在会被拒绝并抛出 `FILE_READ_FAILED` 错误。该行为没有配置项可以恢复为旧版本的处理方式。如果您的 XML 文件由某些工具导出并带有 `DOCTYPE` 头，请在使用 SeaTunnel 读取前将其移除或做预处理。

:::

### csv_use_header_line [boolean]

是否使用标题行解析文件，仅在 file_format 为 `csv` 且文件包含符合 RFC 4180 的标题行时使用

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

**示例 1**：*匹配所有 .txt 文件*，正则表达式：
```
.*.txt
```
此示例匹配的结果是：
```
/data/seatunnel/20241001/report.txt
```
**示例 2**：*匹配所有以 abc 开头的文件*，正则表达式：
```
abc.*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**示例 3**：*匹配20241007文件夹下所有以 abc 开头的文件，且第四个字符为 h 或 g*，正则表达式：
```
/data/seatunnel/20241007/abc[h,g].*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
```
**示例 4**：*匹配以 202410 开头的第三级文件夹和以 .csv 结尾的文件*，正则表达式：
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

过滤文件扩展名，用于过滤具有特定扩展名的文件。示例：`csv` `.txt` `json` `.xml`。

### compress_codec [string]

文件的压缩编解码器及其支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  自动识别压缩类型，无需额外设置。

### archive_compress_codec [string]

归档文件的压缩编解码器及其支持的详细信息如下所示：

| archive_compress_codec | file_format        | archive_compress_suffix |
|------------------------|--------------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

注意：gz 压缩的 excel 文件需要压缩原始文件或指定文件后缀，例如 e2e.xls ->e2e_test.xls.gz

### encoding [string]

仅在 file_format_type 为 json,text,csv,xml 时使用。
要读取的文件的编码。此参数将由 `Charset.forName(encoding)` 解析。

### null_format [string]

仅在 file_format_type 为 text 时使用。
null_format 定义哪些字符串可以表示为 null。

例如：`\N`

### binary_chunk_size [int]

仅在 file_format_type 为 binary 时使用。

读取二进制文件的块大小（以字节为单位）。默认为 1024 字节。较大的值可能会提高大文件的性能，但会使用更多内存。

### binary_complete_file_mode [boolean]

仅在 file_format_type 为 binary 时使用。

是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为 false。

### discovery_mode [string]

文件发现模式，支持：`once`（默认）、`continuous`。

- `once`：启动时枚举一次文件并结束（有界）。
- `continuous`：作业保持运行，周期性扫描路径并在运行时处理新增/变更文件（无界）。

对于 binary 文件，`discovery_mode=continuous` 需要配合 `sync_mode=update`。对于仅追加的 text 文件，请使用 `sync_mode=full`；SeaTunnel 会保存最后一条完整记录的偏移量，并且只读取新追加的完整记录。

### scan_interval [string]

仅在 `discovery_mode=continuous` 时使用。周期性扫描间隔，取值必须大于 `0`。推荐使用简写格式 `10S`、`30S`（大小写不敏感，例如 `10s`）；同时兼容 ISO-8601 格式 `PT10S`、`PT30S`。默认 `10S`。

### start_mode [string]

仅在 `discovery_mode=continuous` 时使用，支持：`earliest`（默认）、`latest`。

- `earliest`：启动时读取已有文件。
- `latest`：仅处理作业启动后修改的新文件。

### sync_mode [string]

文件同步模式，支持：`full`（默认）、`update`。
当 `update` 时，对源/目标进行对比，只读取新增/变更文件（目前仅支持 `file_format_type=binary`）。

**性能注意事项**
- Update 模式会对每个源文件额外发起一次到目标端的 `getFileStatus` 用于对比。
- 不建议用于海量小文件场景。

**要求 / 限制**
- `target_path` 通常应与 sink 的 `path` 一致（同一文件系统且相对路径结构一致）。
- 使用 `update_strategy=distcp` 时，依赖源/目标端时钟同步，否则可能误判。
- 使用 `compare_mode=checksum` 时，需要文件系统支持 checksum；若无法获取 checksum，SeaTunnel 会降级为内容比较（开销更大）并打印告警日志。

示例：

```hocon
sync_mode = "update"
file_format_type = "binary"
target_path = "/path/to/your/sink/path"
update_strategy = "distcp"
compare_mode = "len_mtime"
```

### target_path [string]

仅在 `sync_mode=update` 时使用。目标端基础路径（通常应与 sink 的 `path` 一致），用于对比同相对路径文件。

### target_hadoop_conf [map]

仅在 `sync_mode=update` 时使用。目标端 Hadoop 配置（可选），可在其中设置 `fs.defaultFS` 覆盖目标 defaultFS。

### update_strategy [string]

仅在 `sync_mode=update` 时使用。支持：`distcp`（默认）、`strict`。

### compare_mode [string]

仅在 `sync_mode=update` 时使用。支持：`len_mtime`（默认）、`checksum`（仅在 `update_strategy=strict` 时可用）。

### update_compare_parallelism [int]

`sync_mode=update` 对稀疏目标文件执行元数据点查时的最大并发数。默认值为 `8`，有效范围为 `1` 到 `64`；范围外的值会在配置校验阶段被拒绝；已提交但未完成的任务上限为该值的 8 倍。

### update_compare_bulk_threshold [int]

设置正数后，同一目标父目录的候选文件达到该阈值时，SeaTunnel 改为只枚举一次目标目录。默认值 `0` 表示关闭自动批量枚举，使用有界并发点查，避免意外扫描庞大的目标目录。该行为适用于所有目标文件系统。源端会边枚举边过滤，以降低元数据峰值内存。

### post_sync_action [string]

仅在 `discovery_mode=continuous` 时使用。支持：`none`（默认）、`delete`、`backup`。当 `discovery_mode=once` 时，若配置 `post_sync_action=delete` 或 `post_sync_action=backup`，会在配置校验阶段被显式拒绝。

- `none`：默认行为，不对源文件执行运维动作。
- `delete`：在 `notifyCheckpointComplete` 后删除已处理源文件；失败动作会在后续 checkpoint 回调中重试。
- `backup`：在 `notifyCheckpointComplete` 后将已处理源文件移动到 `backup_path`；失败动作会在后续 checkpoint 回调中重试。

执行 `delete` 或 `backup` 前，SeaTunnel 会先将源文件重命名到 staging/trash 路径，然后重新检查文件长度和修改时间。如果重命名后版本不一致，文件会被恢复到原路径，以便后续扫描重新发现变更后的文件。

**mtime 粒度限制**：某些平台的本地文件系统 mtime 粒度为 1 秒。act-then-verify 方式缩小了但不能完全消除同秒同长度修改的竞态窗口。为了最大安全性，请确保 post-sync 处理期间没有并发写入，或使用 `backup` 而非 `delete` 以便文件可恢复。

### backup_path [string]

仅在 `post_sync_action=backup` 时使用。在 checkpoint 完成提交后，已处理文件会移动到该基础路径，目标文件名会附带源文件版本后缀以避免覆盖冲突。phase-1 仅支持与 `path` 同文件系统（scheme + authority 相同）的 backup，跨文件系统 backup 会被显式拒绝。

`backup_path` 不能与 `path` 相同，不能位于 `path` 之下，`path` 也不能位于 `backup_path` 之下。建议使用专用备份目录，因为保留清理只会管理带 SeaTunnel 版本后缀的备份文件。

### retention_max_age [string]

`backup_path` 的可选保留策略。超过该时长的 SeaTunnel 备份文件会在 checkpoint 完成后的保留扫描中被清理。
仅在 `post_sync_action=backup` 时有效。

支持的时长格式包括带 `MS`、`S`、`M`、`H`、`D` 后缀的简写格式，例如 `500MS`、`30S`、`10M`、`12H`、`7D`，也支持 ISO-8601 时长，例如 `PT1H30M`。

时长后缀不区分大小写：`MS`（毫秒）、`S`（秒）、`M`（分钟）、`H`（小时）、`D`（天）。`M` 始终表示分钟，不是月份。非法值（如 `PT7D`、`P1M`）会导致配置校验失败并报错。

### retention_check_interval [string]

保留清理扫描间隔，默认 `1H`。仅在 `post_sync_action=backup` 且配置 `retention_max_age` 后生效，清理任务最多按该间隔执行一次。单独设置 `retention_check_interval` 不会产生效果。

时长后缀不区分大小写：`MS`、`S`、`M`、`H`、`D`。`M` 始终表示分钟，不是月份。非法值会导致配置校验失败并报错。

### file_filter_modified_start

按照最后修改时间过滤文件。 要过滤的开始时间(包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`。

### file_filter_modified_end

按照最后修改时间过滤文件。 要过滤的结束时间(不包括改时间),时间格式是：`yyyy-MM-dd HH:mm:ss`。

### enable_file_split [boolean]

开启文件分割功能，默认为false。文件类型为csv、text、json、parquet非压缩格式时可选择。

**使用建议**
- 适合：读取少量大文件，并希望通过更高并行度提升吞吐。
- 不建议：读取大量小文件，或并行度较低的场景（拆分会带来额外的枚举/调度开销）。

**限制说明**
- 不支持压缩文件（`compress_codec` != `none`）或归档文件（`archive_compress_codec` != `none`），会自动回退为不拆分。
- 对于 `text`/`csv`/`json`，实际 split 的大小可能略大于 `file_split_size`（因为需要对齐到下一个 `row_delimiter`）。
- LocalFile 内部使用 Hadoop LocalFileSystem（`file:///`），通常不需要额外 Hadoop 配置。

### file_split_size [long]

文件分割大小，enable_file_split参数为true时可以填写。单位是字节数。默认值为128MB的字节数，即134217728。

**调优建议**
- 建议从默认值（128MB）开始：如果并行度未充分利用可适当调小；如果 split 数量过多可适当调大。
- 经验公式：`file_split_size ≈ file_size / 期望并行度`。

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

### 通用选项

数据源插件通用参数，请参阅 [数据源通用选项](../common-options/source-common-options.md) 了解详情

### tables_configs

用于定义多表任务，当您有多个表要读取时，可以使用此选项定义多个表。

## 示例

### 单表

```hocon

LocalFile {
  path = "/apps/hive/demo/student"
  file_format_type = "parquet"
}

```

```hocon

LocalFile {
  schema {
    fields {
      name = string
      age = int
    }
  }
  path = "/apps/hive/demo/student"
  file_format_type = "json"
}

```

对于带有 `encoding` 的 json、text 或 csv 文件格式

```hocon

LocalFile {
    path = "/tmp/hive/warehouse/test2"
    file_format_type = "text"
    encoding = "gbk"
}

```

### 多表

```hocon

LocalFile {
  tables_configs = [
    {
      schema {
        table = "student"
      }
      path = "/apps/hive/demo/student"
      file_format_type = "parquet"
    },
    {
      schema {
        table = "teacher"
      }
      path = "/apps/hive/demo/teacher"
      file_format_type = "parquet"
    }
  ]
}

```

```hocon

LocalFile {
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

### 读取 PDF 文件

```hocon

env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    path = "/data/documents/"
    file_format_type = "pdf"
  }
}

sink {
  Console {
  }
}

```

为获得最佳效果，请使用包含大纲（书签/目录）的 PDF 文件。这使解析器能够提取具有层级信息的标题。

### 传输二进制文件

```hocon

env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
    binary_chunk_size = 2048
    binary_complete_file_mode = false
  }
}
sink {
  // 您可以将本地文件传输到 s3/hdfs/oss 等。
  LocalFile {
    path = "/seatunnel/read/binary2/"
    file_format_type = "binary"
  }
}

```

### 增量同步（sync_mode=update，仅 binary）

`sync_mode=update` 会对比 source 与 `target_path`，仅读取新增/变更文件。
多数情况下，`target_path` 需要与 sink 的 `path` 对齐（同一文件系统、相同相对路径）。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"

    sync_mode = "update"
    target_path = "/seatunnel/read/binary2/"
    update_strategy = "distcp"
    compare_mode = "len_mtime"
  }
}
sink {
  LocalFile {
    path = "/seatunnel/read/binary2/"
    tmp_path = "/seatunnel/read/binary2-tmp/"
    file_format_type = "binary"
  }
}
```

### 持续发现（discovery_mode=continuous）

`discovery_mode=continuous` 会让作业保持运行，并按间隔持续扫描路径发现新/变更文件（长跑作业，推荐使用 `job.mode="STREAMING"`）。

对于 binary 文件，持续发现需要配合 `sync_mode="update"` 使用；`target_path` 通常应与 sink 的 `path` 保持一致（同一文件系统、相同相对路径）。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  LocalFile {
    path = "/seatunnel/watch/src/"
    file_format_type = "binary"

    discovery_mode = "continuous"
    scan_interval = "10S"
    start_mode = "latest"

    sync_mode = "update"
    target_path = "/seatunnel/watch/dst/"
    update_strategy = "distcp"
    compare_mode = "len_mtime"

    post_sync_action = "backup"
    backup_path = "/seatunnel/watch/backup/"
    retention_max_age = "7D"
    retention_check_interval = "1H"
  }
}
sink {
  LocalFile {
    path = "/seatunnel/watch/dst/"
    tmp_path = "/seatunnel/watch/dst-tmp/"
    file_format_type = "binary"
  }
}
```

仅追加的 text 文件可以使用 `sync_mode="full"` 持续读取。source 会等待完整的 `row_delimiter` 后再发送记录，并在 checkpoint 中保存已提交的字节偏移量、本地文件标识和有界内容锚点。`start_mode="earliest"` 会读取已有的完整记录。`start_mode="latest"` 会忽略首次扫描时已有的内容（包括未完成的记录），并从之后新增的完整记录开始读取。

此模式有以下运行约束：

- 仅支持未压缩的 UTF-8 text 文件，并且必须设置 `post_sync_action="none"`。
- 采用至少一次投递语义。reader 确认完整读取一个范围后，source 才会提交该范围。
- 单个文件按顺序读取。source 并行度用于并行读取多个文件，不会拆分同一个文件并行读取。
- 当轮转后的文件仍位于配置路径下并且符合文件过滤条件时，支持重命名后新建文件的轮转方式。通过内容锚点识别 copy-truncate 重写，并从配置的 header 边界重新读取。
- source 文件系统必须为配置路径提供稳定的 `BasicFileAttributes.fileKey()`。如果无法提供（包括默认的 Windows 文件系统 provider），connector 会在启动时拒绝 text tailing，因为创建时间无法安全地区分被替换的文件。
- 内容锚点最多对已提交偏移量之前的前 2 KiB 和后 2 KiB 计算哈希。它可以检测常见的 copy-truncate 重写，但不会检查两个采样区间之间未变化的内容。
- enumerator 和 reader 节点必须能看到相同的文件和文件标识。如果它们可能运行在不同节点，请使用共享挂载目录。
- 文件消失后，其状态会保留三个成功的扫描周期；如果该文件没有等待或运行中的 split，之后会删除该状态。

```hocon
env {
  job.mode = "STREAMING"
}

source {
  LocalFile {
    path = "/var/log/application.log"
    file_format_type = "text"
    schema {
      fields {
        message = "string"
      }
    }
    discovery_mode = "continuous"
    scan_interval = "10S"
    start_mode = "latest"
    sync_mode = "full"
    encoding = "UTF-8"
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
  LocalFile {
    path = "/data/seatunnel/"
    file_format_type = "csv"
    skip_header_row_number = 1
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
