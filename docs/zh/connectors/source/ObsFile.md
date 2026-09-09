import ChangeLog from '../changelog/connector-file-obs.md';

# ObsFile

> Obs 文件源连接器

## 支持这些引擎

> Spark
>
> Flink
>
> Seatunnel Zeta

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读写任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在一次 pollNext 调用中读取分割中的所有数据。读取哪些分割将保存在快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] markdown
  - [x] pdf

## 描述

从华为云 OBS 文件系统读取数据。

如果您使用 spark/flink，为了使用此连接器，您必须确保您的 spark/flink 集群已集成 hadoop。测试的 hadoop 版本是 2.x。

如果您使用 SeaTunnel 引擎，它会在您下载和安装 SeaTunnel 引擎时自动集成 hadoop jar。您可以检查 ${SEATUNNEL_HOME}/lib 下的 jar 包来确认这一点。

我们为了支持更多文件类型做了一些权衡，所以我们使用 HDFS 协议来内部访问 OBS，此连接器需要一些 hadoop 依赖项。
它仅支持 hadoop 版本 **2.9.X+**。

## 必需的 Jar 列表

| jar | 支持的版本 | maven |
|-----|-----------|-------|
| hadoop-huaweicloud | 支持版本 >= 3.1.1.29 | [下载](https://repo.huaweicloud.com/artifactory/sdk_public/org/apache/hadoop/hadoop-huaweicloud/) |
| esdk-obs-java | 支持版本 >= 3.19.7.3 | [下载](https://repo.huaweicloud.com/artifactory/sdk_public/com/huawei/storage/esdk-obs-java/) |
| okhttp | 支持版本 >= 3.11.0 | [下载](https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/) |
| okio | 支持版本 >= 1.14.0 | [下载](https://repo1.maven.org/maven2/com/squareup/okio/okio/) |

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录。
>
> 并将所有 jar 复制到 $SEATUNNEL_HOME/lib/

## 选项

| 参数名                       | 类型      | 必须 | 默认值                 | 描述                                      |
|---------------------------|---------|----|---------------------|-----------------------------------------|
| path                      | string  | 是  | -                   | 目标目录路径                                  |
| file_format_type          | string  | 是  | -                   | 文件类型，支持以下文件类型：`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`                                    |
| bucket                    | string  | 是  | -                   | OBS 文件系统的桶地址，例如：`obs://obs-bucket-name` |
| access_key                | string  | 是  | -                   | OBS 文件系统的访问密钥                           |
| access_secret             | string  | 是  | -                   | OBS 文件系统的访问密钥                           |
| endpoint                  | string  | 是  | -                   | OBS 文件系统的端点                             |
| read_columns              | list    | 否  | -                   | 数据源的读取列列表                               |
| sheet_name                | string  | 否  | -                   | 读取工作簿的工作表，仅在 file_format 为 excel 时使用。                                                                                                                                            |
| excel_engine              | string  | 否  | POI                | 仅在 `file_format` 为 excel 时使用。支持的引擎包括 `POI` 和 `EasyExcel`。                                                                                                                                            |
| poi_excel_max_file_size   | long    | 否  | 52428800           | 仅在 `file_format` 为 excel 且 `excel_engine` 为 POI 时使用。POI 引擎允许读取的最大 Excel 文件大小（默认 50 MB）。                                                                                                                                            |
| delimiter                 | string  | 否  | \001                | 字段分隔符                                   |
| row_delimiter             | string  | 否  | \n                  | 行分隔符                                    |
| parse_partition_from_path | boolean | 否  | true                | 控制是否从文件路径解析分区键和值                        |
| skip_header_row_number    | long    | 否  | 0                   | 跳过前几行，但仅适用于 txt 和 csv。                  |
| date_format               | string  | 否  | yyyy-MM-dd          | 日期类型格式                                  |
| datetime_format           | string  | 否  | yyyy-MM-dd HH:mm:ss | 日期时间类型格式                                |
| time_format               | string  | 否  | HH:mm:ss            | 时间类型格式                                  |
| filename_extension        | string  | 否  | -                   | 使用指定的文件扩展名筛选文件，例如 `csv`、`.txt`、`json` 或 `.xml`。 |
| schema                    | config  | 否  | -                   | 读取 JSON、文本等格式时的字段定义。详见 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| common-options            |         | 否  | -                   | Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。 |
| sheet_name                | string  | 否  | -                   | 读取 Excel 文件时要读取的工作表名称。 |
| file_filter_modified_start | string | 否  | -                   | 按文件最后修改时间筛选文件的起始时间（包含该时间），格式为 `yyyy-MM-dd HH:mm:ss`。 |
| file_filter_modified_end  | string  | 否  | -                   | 按文件最后修改时间筛选文件的结束时间（不包含该时间），格式为 `yyyy-MM-dd HH:mm:ss`。 |
| quote_char                | string  | 否  | "                   | 用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。 |
| escape_char               | string  | 否  | -                   | 用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。        |
| recursive_file_scan       | boolean | 否  | true                | 是否递归扫描子目录。 如果设置为 `false`，将忽略子目录，仅扫描指定路径下的文件。 | 
| sort_files_by_modification_time | boolean | 否 | false               | 是否按修改时间降序排序文件。启用此选项后，在读取不断演化的 schema 时可确保 schema 推断使用最新的文件。                                                                                                                      |

### file_format_type [string]

文件类型，支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `markdown`

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

### sort_files_by_modification_time [boolean]

是否按修改时间降序排序文件。默认值为 `false`。

启用后，文件将按修改时间排序（最新的在前）。适用于以下场景：
- 读取具有不断演化的 schema 的文件，且希望 schema 推断使用最新的文件
- 需要按时间顺序处理文件

### 使用 OBS STS 临时安全凭证读取

生产环境建议通过 [OBS STS](https://support.huaweicloud.com/intl/zh-cn/api-obs/obs_04_0081.html) 颁发临时 AK/SK，并配合细粒度自定义策略限制只能访问指定 bucket 前缀，再通过 `hadoop_obs_properties` 传给连接器。

```hocon
source {
  ObsFile {
    path = "/staging/prefix"
    bucket = "obs://target-bucket"
    endpoint = "obs.ap-southeast-1.myhuaweicloud.com"
    hadoop_obs_properties = {
      "fs.obs.access.key"    = "<临时-access-key>"
      "fs.obs.secret.key"    = "<临时-secret-key>"
      "fs.obs.session.token" = "<临时-security-token>"
    }
    file_format_type = "parquet"
  }
}
```

provider 的 jar 必须放在每个运行节点的 classpath（`${SEATUNNEL_HOME}/lib`）上。生产环境应避免在作业配置中硬编码长期 AK/SK；运行在华为云内时推荐使用 ECS 委托（Agency）或 STS 临时凭证。

## 变更日志

<ChangeLog />
