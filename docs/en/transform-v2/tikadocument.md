# TikaDocument

> TikaDocument Transform Plugin

## Description

The `TikaDocument` transform plugin uses Apache Tika to extract text content and metadata from various document formats including PDF, Microsoft Office documents (Word, Excel, PowerPoint), plain text, HTML, XML, and many other file formats. This transform converts binary document data into structured text content and metadata fields.

The plugin supports comprehensive error handling, content processing options, and can handle both binary data and Base64-encoded document content.

## Options

| Name                               | Type   | Required | Default Value | Description                                                                                           |
|------------------------------------|--------|----------|---------------|-------------------------------------------------------------------------------------------------------|
| source_field                       | string | yes      | -             | The name of the source field containing document data (binary or Base64)                             |
| output_fields                      | map    | no       | auto-generated| Mapping of extracted content to output field names                                                    |
| parse_options.extract_text         | bool   | no       | true          | Whether to extract text content from documents                                                        |
| parse_options.extract_metadata     | bool   | no       | true          | Whether to extract document metadata                                                                  |
| parse_options.max_string_length    | int    | no       | 10000         | Maximum length of extracted text content                                                              |
| content_processing.remove_empty_lines | bool | no       | false         | Whether to remove empty lines from extracted text                                                     |
| content_processing.trim_whitespace | bool   | no       | false         | Whether to trim whitespace from extracted text                                                        |
| content_processing.normalize_whitespace | bool | no     | false         | Whether to normalize multiple whitespaces to single spaces                                            |
| content_processing.min_content_length | int  | no       | 0             | Minimum content length threshold (shorter content will be skipped)                                    |
| error_handling.on_parse_error      | enum   | no       | skip          | How to handle document parsing errors: `fail`, `skip`, `null`                                        |
| error_handling.on_unsupported_format | enum | no       | skip          | How to handle unsupported document formats: `fail`, `skip`, `null`                                   |
| error_handling.log_errors          | bool   | no       | false         | Whether to log error messages                                                                         |
| timeout_ms                         | long   | no       | 30000         | Timeout for document processing in milliseconds                                                       |

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

### source_field [string]

The name of the input field that contains the document data. This field should contain either:
- Binary document data (byte array)
- Base64-encoded document data (string)

### output_fields [map]

A mapping that specifies which extracted fields should be output and their corresponding field names. If not specified, the plugin will automatically generate output fields based on the parsing options.

**Default output fields:**
```hocon
output_fields {
    content = "extracted_text"        # Extracted text content
    content_type = "mime_type"        # MIME type of the document  
    title = "doc_title"               # Document title (if available)
}
```

**Custom output fields:**
```hocon
output_fields {
    content = "document_content"
    content_type = "file_type"
    title = "document_title"
    author = "document_author"
    subject = "document_subject"
    keywords = "document_keywords"
    language = "document_language"
    created_date = "creation_date"
    modified_date = "modification_date"
    metadata = "all_metadata"
}
```

### parse_options

#### extract_text [bool]

Whether to extract text content from documents. When enabled, the plugin will extract readable text from the document.

#### extract_metadata [bool]

Whether to extract document metadata such as title, author, creation date, etc.

#### max_string_length [int]

Maximum length of extracted text content. Text longer than this limit will be truncated.

### content_processing

#### remove_empty_lines [bool]

Whether to remove empty lines from the extracted text content.

#### trim_whitespace [bool]

Whether to trim leading and trailing whitespace from the extracted text.

#### normalize_whitespace [bool]

Whether to normalize multiple consecutive whitespace characters to single spaces.

#### min_content_length [int]

Minimum length threshold for extracted content. Content shorter than this length will be considered invalid and handled according to the error handling strategy.

### error_handling

#### on_parse_error [enum]

Specifies how to handle document parsing errors:
- `fail`: Throw an exception and stop processing
- `skip`: Skip the current row and continue processing
- `null`: Fill output fields with null values

#### on_unsupported_format [enum]

Specifies how to handle unsupported document formats:
- `fail`: Throw an exception and stop processing
- `skip`: Skip the current row and continue processing  
- `null`: Fill output fields with null values

#### log_errors [bool]

Whether to log detailed error messages when processing failures occur.

### timeout_ms [long]

Timeout for document processing in milliseconds. If document processing takes longer than this timeout, it will be terminated and handled according to the error handling strategy.

## Supported Document Formats

The TikaDocument transform supports a wide variety of document formats through Apache Tika:

- **Text formats**: TXT, RTF, CSV
- **PDF documents**: PDF
- **Microsoft Office**: DOC, DOCX, XLS, XLSX, PPT, PPTX
- **OpenOffice/LibreOffice**: ODT, ODS, ODP
- **Web formats**: HTML, XML, XHTML
- **Archive formats**: ZIP, TAR, GZIP
- **Image formats** (with OCR if available): JPEG, PNG, TIFF, GIF
- **Email formats**: MSG, EML, MBOX
- **eBook formats**: EPUB, MOBI
- **And many more**

## Examples

### Basic Document Processing

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

### Advanced Configuration with Content Processing

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

### Multi-table Processing

```hocon
transform {
  TikaDocument {
    source_field = "document_data"
    output_fields = {
      content = "extracted_content"
      content_type = "document_type"
    }
    multi_tables = true
  }
}
```

## Data Type Mapping

| Input Type | Output Type | Description |
|------------|-------------|-------------|
| BYTES      | STRING      | Binary document data → Extracted text content |
| STRING     | STRING      | Base64 document data → Extracted text content |

Output fields data types:
- `content`: STRING (extracted text)
- `content_type`: STRING (MIME type)
- `title`: STRING (document title)
- `author`: STRING (document author)
- `subject`: STRING (document subject)
- `keywords`: STRING (document keywords)
- `language`: STRING (document language)
- `created_date`: STRING (creation date in ISO format)
- `modified_date`: STRING (modification date in ISO format)
- `metadata`: MAP<STRING, STRING> (all metadata as key-value pairs)

## Performance Considerations

- **Memory Usage**: Large documents may consume significant memory during processing
- **Processing Time**: Complex documents (especially PDFs with images) may take longer to process
- **Timeout Settings**: Adjust `timeout_ms` based on your document sizes and processing requirements
- **Batch Size**: For high-volume processing, consider adjusting batch sizes to balance memory usage and throughput

## Error Handling Best Practices

1. **Use appropriate error handling strategies** based on your use case:
   - `fail`: For critical pipelines where document processing must succeed
   - `skip`: For batch processing where some failures are acceptable
   - `null`: When you want to preserve row structure but mark failed extractions

2. **Enable logging** during development and testing to understand processing issues

3. **Set reasonable timeouts** to prevent hanging on corrupted or very large documents

4. **Monitor extraction success rates** in production environments

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**: Reduce `max_string_length` or increase JVM heap size
2. **Timeout issues**: Increase `timeout_ms` for large documents
3. **Unsupported formats**: Check document format support or use appropriate error handling
4. **Encoding issues**: Ensure proper character encoding for text documents

### Debug Tips

- Enable `log_errors = true` to see detailed error messages
- Use `on_parse_error = "null"` to identify problematic documents
- Test with small document samples first
- Verify document integrity before processing
