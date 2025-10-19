# TikaDocument

> TikaDocument 转换插件

## 描述

`TikaDocument` 转换插件使用 [Apache Tika](https://tika.apache.org/) 从各种文档格式中提取文本内容和元数据，包括 PDF、Microsoft Office 文档（Word、Excel、PowerPoint）、纯文本、HTML、XML 和许多其他文件格式。该转换将二进制文档数据转换为结构化的文本内容和元数据字段。

该插件支持全面的错误处理、内容处理选项，并可以处理二进制数据和 Base64 编码的文档内容。

有关 Apache Tika 和支持的格式的更多信息，请参见 [Apache Tika 文档](https://tika.apache.org/2.9.2/index.html)。

### 适用的数据源连接器

此转换设计用于能够以以下格式之一提供文档数据的源连接器：

**二进制文档数据（字节数组）**：
- **文件类源**：`LocalFile`、`S3File`、`HDFS`、`FTP`、`SFTP` - 以二进制数据读取文件
- **JDBC 源**：从数据库读取 `BLOB` 或 `VARBINARY` 列时
- **MongoDB**：从 GridFS 或二进制字段读取二进制文档数据
- **对象存储源**：`OSS`、`COS`、`OBS` - 以字节形式读取文件内容

**Base64 编码的字符串**：
- **Kafka**：包含 Base64 编码文档数据的消息
- **HTTP 源**：包含 Base64 编码文件的 API 响应
- **数据库文本字段**：存储为 Base64 字符串的文档，存放在 VARCHAR/TEXT 列中

**源配置示例**：

从 S3 读取 PDF 文件：
```hocon
source {
  S3File {
    path = "s3a://bucket/documents/*.pdf"
    file_format_type = "binary"
    schema = {
      fields {
        document_data = bytes
        filename = string
      }
    }
  }
}
```

从数据库 BLOB 列读取文档：
```hocon
source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/doc_db"
    query = "SELECT id, document_blob, filename FROM documents"
    # document_blob 将以字节数组形式读取
  }
}
```

从 Kafka 读取 Base64 编码的文档：
```hocon
source {
  Kafka {
    topic = "documents"
    bootstrap.servers = "localhost:9092"
    schema = {
      fields {
        doc_id = string
        document_data = string  # Base64 编码
      }
    }
  }
}
```

## 属性

| 名称                                 | 类型     | 是否必须 | 默认值        | 描述                                                                                                  |
|------------------------------------|--------|------|------------|-----------------------------------------------------------------------------------------------------|
| source_field                       | string | 是    | -          | 包含文档数据（二进制或 Base64）的源字段名称                                                                         |
| output_fields                      | map    | 否    | 自动生成       | 提取内容到输出字段名称的映射                                                                                      |
| parse_options.extract_text         | bool   | 否    | true       | 是否从文档中提取文本内容                                                                                        |
| parse_options.extract_metadata     | bool   | 否    | true       | 是否提取文档元数据                                                                                           |
| parse_options.max_string_length    | int    | 否    | 10000      | 提取的文本内容的最大长度                                                                                        |
| content_processing.remove_empty_lines | bool | 否    | false      | 是否从提取的文本中移除空行                                                                                       |
| content_processing.trim_whitespace | bool   | 否    | false      | 是否修剪提取文本的空白字符                                                                                       |
| content_processing.normalize_whitespace | bool | 否  | false      | 是否将多个空白字符标准化为单个空格                                                                                   |
| content_processing.min_content_length | int  | 否    | 0          | 最小内容长度阈值（较短的内容将被跳过）                                                                                 |
| error_handling.on_parse_error      | enum   | 否    | skip       | 如何处理文档解析错误：`fail`、`skip`、`null`                                                                      |
| error_handling.on_unsupported_format | enum | 否    | skip       | 如何处理不支持的文档格式：`fail`、`skip`、`null`                                                                   |
| error_handling.log_errors          | bool   | 否    | false      | 是否记录错误消息                                                                                            |
| timeout_ms                         | long   | 否    | 30000      | 文档处理超时时间（毫秒）                                                                                        |

### common options [string]

转换插件的常见参数，请参考 [Transform Plugin](common-options.md) 了解详情

### source_field [string]

包含文档数据的输入字段名称。该字段应包含以下类型之一：
- 二进制文档数据（字节数组）
- Base64 编码的文档数据（字符串）

### output_fields [map]

指定应输出哪些提取字段及其对应字段名称的映射。如果未指定，插件将根据解析选项自动生成输出字段。

**默认输出字段：**
```hocon
output_fields {
    content = "extracted_text"        # 提取的文本内容
    content_type = "mime_type"        # 文档的 MIME 类型  
    title = "doc_title"               # 文档标题（如果可用）
}
```

**自定义输出字段：**
```hocon
output_fields {
    content = "document_content"      # 文档内容
    content_type = "file_type"        # 文件类型
    title = "document_title"          # 文档标题
    author = "document_author"        # 文档作者
    subject = "document_subject"      # 文档主题
    keywords = "document_keywords"    # 文档关键词
    language = "document_language"    # 文档语言
    created_date = "creation_date"    # 创建日期
    modified_date = "modification_date" # 修改日期
    metadata = "all_metadata"         # 所有元数据
}
```

### parse_options

#### extract_text [bool]

是否从文档中提取文本内容。启用时，插件将从文档中提取可读的文本。

#### extract_metadata [bool]

是否提取文档元数据，如标题、作者、创建日期等。

#### max_string_length [int]

提取的文本内容的最大长度。超过此限制的文本将被截断。

### content_processing

#### remove_empty_lines [bool]

是否从提取的文本内容中删除空行。

#### trim_whitespace [bool]

是否修剪提取文本的前导和尾随空白字符。

#### normalize_whitespace [bool]

是否将多个连续的空白字符标准化为单个空格。

#### min_content_length [int]

提取内容的最小长度阈值。短于此长度的内容将被视为无效，并根据错误处理策略进行处理。

### error_handling

#### on_parse_error [enum]

指定如何处理文档解析错误：
- `fail`：抛出异常并停止处理
- `skip`：跳过当前行并继续处理
- `null`：用 null 值填充输出字段

#### on_unsupported_format [enum]

指定如何处理不支持的文档格式：
- `fail`：抛出异常并停止处理
- `skip`：跳过当前行并继续处理
- `null`：用 null 值填充输出字段

#### log_errors [bool]

是否在发生处理失败时记录详细的错误消息。

### timeout_ms [long]

文档处理超时时间（毫秒）。如果文档处理时间超过此超时时间，将被终止并根据错误处理策略进行处理。

## 支持的文档格式

TikaDocument 转换通过 Apache Tika 支持多种文档格式：

- **文本格式**：TXT、RTF、CSV
- **PDF 文档**：PDF
- **Microsoft Office**：DOC、DOCX、XLS、XLSX、PPT、PPTX
- **OpenOffice/LibreOffice**：ODT、ODS、ODP
- **网页格式**：HTML、XML、XHTML
- **压缩格式**：ZIP、TAR、GZIP
- **图像格式**（如果支持 OCR）：JPEG、PNG、TIFF、GIF
- **邮件格式**：MSG、EML、MBOX
- **电子书格式**：EPUB、MOBI
- **以及更多格式**

## 示例

### 基本文档处理

```hocon
transform {
  TikaDocument {
    source_field = "document_data"
    output_fields = {
      content = "extracted_text"
      content_type = "mime_type"
    }
  }
}
```

### 高级配置与内容处理

```hocon
transform {
  TikaDocument {
    source_field = "file_content"
    output_fields = {
      content = "document_text"
      content_type = "file_type"
      title = "doc_title"
      author = "doc_author"
      metadata = "all_metadata"
    }
    parse_options = {
      extract_text = true
      extract_metadata = true
      max_string_length = 50000
    }
    content_processing = {
      remove_empty_lines = true
      trim_whitespace = true
      normalize_whitespace = true
      min_content_length = 10
    }
    error_handling = {
      on_parse_error = "skip"
      on_unsupported_format = "null"
      log_errors = true
    }
    timeout_ms = 60000
  }
}
```

### 多表处理

当处理来自多个表的文档（例如，来自 MySQL-CDC 或 JDBC 多表源）时，您可以使用标准的多表转换配置：

```hocon
transform {
  TikaDocument {
    source_field = "document_data"
    output_fields = {
      content = "extracted_content"
      content_type = "document_type"
    }
    # 匹配所有需要文档提取的表
    table_match_regex = "doc_db\\..*"
    
    # 可选：为特定表指定不同的配置
    table_transform = [{
      table_path = "doc_db.pdf_documents"
      source_field = "pdf_data"
      parse_options = {
        max_string_length = 100000
      }
    }, {
      table_path = "doc_db.office_documents"
      source_field = "doc_content"
      parse_options = {
        max_string_length = 50000
      }
    }]
  }
}
```

有关多表转换的更多详细信息，请参阅 [多表转换](transform-multi-table.md)。

## 数据类型映射

| 输入类型   | 输出类型   | 描述                    |
|--------|--------|----------------------|
| BYTES  | STRING | 二进制文档数据 → 提取的文本内容    |
| STRING | STRING | Base64 文档数据 → 提取的文本内容 |

输出字段数据类型：
- `content`：STRING（提取的文本）
- `content_type`：STRING（MIME 类型）
- `title`：STRING（文档标题）
- `author`：STRING（文档作者）
- `subject`：STRING（文档主题）
- `keywords`：STRING（文档关键词）
- `language`：STRING（文档语言）
- `created_date`：TIMESTAMP（创建日期）
- `modified_date`：TIMESTAMP（修改日期）
- `page_count`：INT（页数）
- `file_size`：LONG（文档大小，字节）
- `metadata`：MAP<STRING, STRING>（所有元数据作为键值对）

## 元数据架构集成

TikaDocument 转换会自动将所有提取的文档元数据字段添加到输出目录表的**元数据架构**中。这允许下游转换使用 `Metadata` 转换以元数据的形式访问这些字段。

**可用的元数据字段：**
- `content` - 提取的文本内容
- `content_type` - 文档的 MIME 类型
- `title` - 文档标题
- `author` - 文档作者/创建者
- `subject` - 文档主题
- `keywords` - 文档关键词
- `language` - 检测到的语言
- `created_date` - 创建日期
- `modified_date` - 修改日期
- `page_count` - 页数
- `file_size` - 文档大小（字节）
- `metadata` - 所有元数据作为键值对

**注意：** 在特定文档中不存在的字段将设置为 `null`。例如，纯文本文件可能没有 `author` 或 `title` 元数据。

### 在下游转换中使用元数据

您可以使用 `Metadata` 转换将这些文档元数据字段映射到输出列：

```hocon
transform {
  # 首先，提取文档内容
  TikaDocument {
    source_field = "document_data"
    output_fields = {
      content = "extracted_text"
    }
  }
  
  # 然后，使用 Metadata 转换访问文档元数据
  Metadata {
    metadata_fields = {
      title = "doc_title"
      author = "doc_author"
      content_type = "mime_type"
      created_date = "creation_timestamp"
    }
  }
}
```

## 性能考虑

- **内存使用**：大型文档在处理过程中可能消耗大量内存
- **处理时间**：复杂文档（特别是含有图像的 PDF）可能需要更长的处理时间
- **超时设置**：根据文档大小和处理要求调整 `timeout_ms`
- **批处理大小**：对于高容量处理，考虑调整批处理大小以平衡内存使用和吞吐量

## 错误处理最佳实践

1. **根据使用场景选择适当的错误处理策略**：
   - `fail`：对于文档处理必须成功的关键管道
   - `skip`：对于可以接受某些失败的批处理
   - `null`：当您想保留行结构但标记失败的提取时

2. **在开发和测试期间启用日志记录** 以了解处理问题

3. **设置合理的超时时间** 以防止在损坏或非常大的文档上挂起

4. **在生产环境中监控提取成功率**

## 故障排除

### 常见问题

1. **OutOfMemoryError**：减少 `max_string_length` 或增加 JVM 堆大小
2. **超时问题**：为大型文档增加 `timeout_ms`
3. **不支持的格式**：检查文档格式支持或使用适当的错误处理
4. **编码问题**：确保文本文档的字符编码正确

### 调试技巧

- 启用 `log_errors = true` 查看详细错误消息
- 使用 `on_parse_error = "null"` 识别有问题的文档
- 首先用小文档样本进行测试
- 在处理前验证文档完整性
