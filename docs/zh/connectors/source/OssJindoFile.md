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
- [x] [多模态](../../introduction/concepts/connector-v2-features.md#multimodal)

  使用二进制文件格式读写任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在一次 pollNext 调用中读取分割中的所有数据。读取哪些分割将保存在快照中。

- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
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
| file_format_type          | string  | 是  | -                           | 文件类型                                                                          |
| bucket                    | string  | 是  | -                           | OSS 文件系统的桶地址                                                                  |
| access_key                | string  | 是  | -                           | OSS 文件系统的访问密钥                                                                 |
| access_secret             | string  | 是  | -                           | OSS 文件系统的访问密钥                                                                 |
| endpoint                  | string  | 是  | -                           | OSS 文件系统的端点                                                                   |
| read_columns              | list    | 否  | -                           | 数据源的读取列列表                                                                     |
| delimiter/field_delimiter | string  | 否  | \001 for text and , for csv | 字段分隔符                                                                         |
| row_delimiter             | string  | 否  | \n                          | 行分隔符                                                                          |
| parse_partition_from_path | boolean | 否  | true                        | 控制是否从文件路径解析分区键和值                                                              |
| date_format               | string  | 否  | yyyy-MM-dd                  | 日期类型格式                                                                        |
| datetime_format           | string  | 否  | yyyy-MM-dd HH:mm:ss         | 日期时间类型格式                                                                      |
| time_format               | string  | 否  | HH:mm:ss                    | 时间类型格式                                                                        |
| skip_header_row_number    | long    | 否  | 0                           | 跳过前几行                                                                         |
| schema                    | config  | 否  | -                           | 上游数据的模式信息。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| sheet_name                | string  | 否  | -                           | Excel 工作表名称                                                                   |
| xml_row_tag               | string  | 否  | -                           | XML 行标签                                                                       |
| xml_use_attr_format       | boolean | 否  | -                           | 是否使用 XML 属性格式                                                                 |
| csv_use_header_line       | boolean | 否  | false                       | 是否使用 CSV 标题行                                                                  |
| file_filter_pattern       | string  | 否  | -                           | 文件过滤模式                                                                        |
| quote_char                | string  | 否  | "                           | 用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。                                       |
| escape_char               | string  | 否  | -                           | 用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。                                              |
| sort_files_by_modification_time | boolean | 否 | false               | 是否按修改时间降序排序文件。启用此选项后，在读取不断演化的 schema 时可确保 schema 推断使用最新的文件。                                                                                                                      |

### sort_files_by_modification_time [boolean]

是否按修改时间降序排序文件。默认值为 `false`。

启用后，文件将按修改时间排序（最新的在前）。适用于以下场景：
- 读取具有不断演化的 schema 的文件，且希望 schema 推断使用最新的文件
- 需要按时间顺序处理文件

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

当 `markdown_rag_metadata_enabled` 设置为 `true` 时，SeaTunnel 会在 `child_ids` 之后追加以下 RAG 元数据字段：
- `source_uri`：源文件路径或 URI
- `document_id`：由 `source_uri` 派生的稳定文档标识符
- `chunk_id`：由文档标识、chunk 顺序和内容哈希派生的稳定 chunk 标识符
- `chunk_index`：解析后文档中的一基 chunk 顺序
- `content_hash`：已输出 `text` 值的 SHA-256 哈希

该选项默认值为 `false`，因此只有显式启用后才会改变原始 Markdown schema。

注意：Markdown 格式仅支持读取，不支持写入。

## 变更日志

<ChangeLog />

