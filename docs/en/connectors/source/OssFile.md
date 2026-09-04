import ChangeLog from '../changelog/connector-file-oss.md';

# OssFile

> Oss file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Usage Dependency

### For Spark/Flink Engine

1. You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.
2. You must ensure `hadoop-aliyun-xx.jar`, `aliyun-sdk-oss-xx.jar` and `jdom-xx.jar` in `${SEATUNNEL_HOME}/plugins/` dir and the version of `hadoop-aliyun` jar need equals your hadoop version which used in spark/flink and `aliyun-sdk-oss-xx.jar` and `jdom-xx.jar` version needs to be the version corresponding to the `hadoop-aliyun` version. Eg: `hadoop-aliyun-3.1.4.jar` dependency `aliyun-sdk-oss-3.4.1.jar` and `jdom-1.1.jar`.

### For SeaTunnel Zeta Engine

1. You must ensure `seatunnel-shade-hadoop3-uber-3.1.4-3.0.0.jar`, `aliyun-sdk-oss-3.4.1.jar`, `hadoop-aliyun-3.1.4.jar` and `jdom-1.1.jar` in `${SEATUNNEL_HOME}/lib/` dir.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

  Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] file format type
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

## Data Type Mapping

Data type mapping is related to the type of file being read, We supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `markdown`

### JSON File Type

If you assign file type to `json`, you should also assign schema option to tell connector how to parse data to the row you want.

For example:

upstream data is the following:

```json

{"code":  200, "data":  "get success", "success":  true}

```

You can also save multiple pieces of data in one file and split them by newline:

```json lines

{"code":  200, "data":  "get success", "success":  true}
{"code":  300, "data":  "get failed", "success":  false}

```

you should assign schema as the following:

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

connector will generate data as the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

### Text Or CSV File Type

If you set the `file_format_type` to `text`,`excel`,`csv`,`xml`. Then it's required to set the `schema` field to tell connector how to parse data to the row.

If you set the `schema` field, you should also set the option `field_delimiter`, except the `file_format_type` is `csv`, `xml`, `excel`

you can set schema and delimiter as the following:

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

connector will generate data as the following:

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

### Orc File Type

If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.

|          Orc Data type           |                      SeaTunnel Data type                       |
|----------------------------------|----------------------------------------------------------------|
| BOOLEAN                          | BOOLEAN                                                        |
| INT                              | INT                                                            |
| BYTE                             | BYTE                                                           |
| SHORT                            | SHORT                                                          |
| LONG                             | LONG                                                           |
| FLOAT                            | FLOAT                                                          |
| DOUBLE                           | DOUBLE                                                         |
| BINARY                           | BINARY                                                         |
| STRING<br/>VARCHAR<br/>CHAR<br/> | STRING                                                         |
| DATE                             | LOCAL_DATE_TYPE                                                |
| TIMESTAMP                        | LOCAL_DATE_TIME_TYPE                                           |
| DECIMAL                          | DECIMAL                                                        |
| LIST(STRING)                     | STRING_ARRAY_TYPE                                              |
| LIST(BOOLEAN)                    | BOOLEAN_ARRAY_TYPE                                             |
| LIST(TINYINT)                    | BYTE_ARRAY_TYPE                                                |
| LIST(SMALLINT)                   | SHORT_ARRAY_TYPE                                               |
| LIST(INT)                        | INT_ARRAY_TYPE                                                 |
| LIST(BIGINT)                     | LONG_ARRAY_TYPE                                                |
| LIST(FLOAT)                      | FLOAT_ARRAY_TYPE                                               |
| LIST(DOUBLE)                     | DOUBLE_ARRAY_TYPE                                              |
| Map<K,V>                         | MapType, This type of K and V will transform to SeaTunnel type |
| STRUCT                           | SeaTunnelRowType                                               |

### Parquet File Type

If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.

| Parquet Data type    | SeaTunnel Data type                                            |
|----------------------|----------------------------------------------------------------|
| INT_8                | BYTE                                                           |
| INT_16               | SHORT                                                          |
| DATE                 | DATE                                                           |
| TIMESTAMP_MILLIS     | TIMESTAMP                                                      |
| INT64                | LONG                                                           |
| INT96                | TIMESTAMP                                                      |
| BINARY               | BYTES                                                          |
| FLOAT                | FLOAT                                                          |
| DOUBLE               | DOUBLE                                                         |
| BOOLEAN              | BOOLEAN                                                        |
| FIXED_LEN_BYTE_ARRAY | TIMESTAMP<br/> DECIMAL                                         |
| DECIMAL              | DECIMAL                                                        |
| LIST(STRING)         | STRING_ARRAY_TYPE                                              |
| LIST(BOOLEAN)        | BOOLEAN_ARRAY_TYPE                                             |
| LIST(TINYINT)        | BYTE_ARRAY_TYPE                                                |
| LIST(SMALLINT)       | SHORT_ARRAY_TYPE                                               |
| LIST(INT)            | INT_ARRAY_TYPE                                                 |
| LIST(BIGINT)         | LONG_ARRAY_TYPE                                                |
| LIST(FLOAT)          | FLOAT_ARRAY_TYPE                                               |
| LIST(DOUBLE)         | DOUBLE_ARRAY_TYPE                                              |
| Map<K,V>             | MapType, This type of K and V will transform to SeaTunnel type |
| STRUCT               | SeaTunnelRowType                                               |

## Options

| name                       | type    | required | default value       | Description                                                                                                                                                                                                                                                                                                                         |
|----------------------------|---------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                       | string  | yes      | -                   | The Oss path that needs to be read can have sub paths, but the sub paths need to meet certain format requirements. Specific requirements can be referred to "parse_partition_from_path" option                                                                                                                                      |
| file_format_type           | string  | yes      | -                   | File type, supported as the following file types: `text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`                                                                                                                                                                                                             |
| bucket                     | string  | yes      | -                   | The bucket address of oss file system, for example: `oss://seatunnel-test`.                                                                                                                                                                                                                                                         |
| endpoint                   | string  | yes      | -                   | fs oss endpoint                                                                                                                                                                                                                                                                                                                     |
| read_columns               | list    | no       | -                   | The read column list of the data source, user can use it to implement field projection. The file type supported column projection as the following shown: `text` `csv` `parquet` `orc` `json` `excel` `xml` . If the user wants to use this feature when reading `text` `json` `csv` files, the "schema" option must be configured. |
| access_key                 | string  | no       | -                   |                                                                                                                                                                                                                                                                                                                                     |
| access_secret              | string  | no       | -                   |                                                                                                                                                                                                                                                                                                                                     |
| delimiter                  | string  | no       | \001                | Field delimiter, used to tell connector how to slice and dice fields when reading text files. Default `\001`, the same as hive's default delimiter.                                                                                                                                                                                 |
| row_delimiter              | string  | no       | \n                  | Row delimiter, used to tell connector how to slice and dice rows when reading text files. Default `\n`.                                                                                                                                                                                                                             |
| parse_partition_from_path  | boolean | no       | true                | Control whether parse the partition keys and values from file path. For example if you read a file from path `oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`. Every record data from file will be added these two fields: name="tyrantlucifer", age=16                                                       |
| date_format                | string  | no       | yyyy-MM-dd          | Date type format, used to tell connector how to convert string to date, supported as the following formats:`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`. default `yyyy-MM-dd`                                                                                                                                                             |
| datetime_format            | string  | no       | yyyy-MM-dd HH:mm:ss | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats:`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`                                                                                                                               |
| time_format                | string  | no       | HH:mm:ss            | Time type format, used to tell connector how to convert string to time, supported as the following formats:`HH:mm:ss` `HH:mm:ss.SSS`                                                                                                                                                                                                |
| filename_extension         | string  | no       | -                   | Filter filename extension, which used for filtering files with specific extension. Example: `csv` `.txt` `json` `.xml`.                                                                                                                                                                                                             |
| skip_header_row_number     | long    | no       | 0                   | Skip the first few lines, but only for the txt and csv. For example, set like following:`skip_header_row_number = 2`. Then SeaTunnel will skip the first 2 lines from source files                                                                                                                                                  |
| schema                     | config  | no       | -                   | The schema of upstream data.                                                                                                                                                                                                                                                                                                        |
| sheet_name                 | string  | no       | -                   | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                               |
| excel_engine               | string  | no       | POI                 | Only used when `file_format` is excel. Supported engines are `POI` and `EasyExcel`.                                                                                                                                                                                                                                                 |
| poi_excel_max_file_size    | long    | no       | 52428800            | Only used when `file_format` is excel and `excel_engine` is POI. The maximum Excel file size in bytes that the POI engine can read (default 50 MB).                                                                                                                                                                                 |
| xml_row_tag                | string  | no       | -                   | Specifies the tag name of the data rows within the XML file, only used when file_format is xml.                                                                                                                                                                                                                                     |
| xml_use_attr_format        | boolean | no       | -                   | Specifies whether to process data using the tag attribute format, only used when file_format is xml.                                                                                                                                                                                                                                |
| csv_use_header_line        | boolean | no       | false               | Whether to use the header line to parse the file, only used when the file_format is `csv` and the file contains the header line that match RFC 4180                                                                                                                                                                                 |
| compress_codec             | string  | no       | none                | Which compress codec the files used.                                                                                                                                                                                                                                                                                                |
| encoding                   | string  | no       | UTF-8               | File encoding, only used when `file_format_type` is `json`, `text`, `csv`, or `xml`.                                                                                                                                                                                                                                                |
| null_format                | string  | no       | -                   | Only used when file_format_type is text. null_format to define which strings can be represented as null. e.g: `\N`                                                                                                                                                                                                                  |
| binary_chunk_size          | int     | no       | 1024                | Only used when file_format_type is binary. The chunk size (in bytes) for reading binary files. Default is 1024 bytes. Larger values may improve performance for large files but use more memory.                                                                                                                                    |
| binary_complete_file_mode  | boolean | no       | false               | Only used when file_format_type is binary. Whether to read the complete file as a single chunk instead of splitting into chunks. When enabled, the entire file content will be read into memory at once. Default is false.                                                                                                          |
| discovery_mode             | string  | no       | once                | File discovery mode. Supported values: `once` (default), `continuous`. Continuous mode periodically scans the path and currently requires `sync_mode=update` and `file_format_type=binary`. |
| scan_interval              | string  | no       | 10S                 | Polling interval used when `discovery_mode=continuous`. Shorthand values such as `10S` and ISO-8601 values such as `PT10S` are supported. |
| start_mode                 | string  | no       | earliest            | Initial scan behavior for continuous discovery. `earliest` processes existing files; `latest` processes only later additions or changes. |
| sync_mode                  | string  | no       | full                | File sync mode. `update` compares source objects with `target_path` and reads only new or changed objects. Update mode currently supports binary format only. |
| target_path                | string  | no       | -                   | Required when `sync_mode=update`. Target base path used for comparison and normally the same as the sink `path`. |
| target_hadoop_conf         | map     | no       | -                   | Optional Hadoop configuration for the target filesystem when `sync_mode=update`. |
| update_strategy            | string  | no       | distcp              | Comparison strategy used by update mode. Supported values are `distcp` and `strict`. |
| compare_mode               | string  | no       | len_mtime           | Comparison mode used by update mode. Supported values are `len_mtime` and `checksum`; checksum requires `update_strategy=strict`. |
| update_compare_parallelism | int     | no       | 8                   | Maximum parallelism for target metadata lookups. Valid range is `1-64`. |
| update_compare_bulk_threshold | int  | no       | 0                   | Positive values switch comparison to a directory listing when that many candidates share a target parent. `0` disables automatic bulk listing. |
| post_sync_action           | string  | no       | none                | Optional action after a continuously discovered object is checkpointed. Supported values are `none`, `delete`, and `backup`. |
| backup_path                | string  | no       | -                   | Required when `post_sync_action=backup`. Backup destination must not overlap with the source `path`. |
| retention_max_age          | string  | no       | -                   | Optional maximum age for SeaTunnel backup objects under `backup_path`. |
| retention_check_interval   | string  | no       | 1H                  | Retention scan interval when backup retention is configured. |
| file_filter_pattern        | string  | no       |                     | Filter pattern, which used for filtering files.                                                                                                                                                                                                                                                                                     |
| common-options             | config  | no       | -                   | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                                                                                                                  |
| file_filter_modified_start | string  | no       | -                   | File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                |
| file_filter_modified_end   | string  | no       | -                   | File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                |
| quote_char                 | string  | no       | "                   | A single character that encloses CSV fields, allowing fields with commas, line breaks, or quotes to be read correctly.                                                                                                                                                                                                              |
| escape_char                | string  | no       | -                   | A single character that allows the quote or other special characters to appear inside a CSV field without ending the field.                                                                                                                                                                                                         |
| metalake_type              | string  | no       | gravitino          | The type of metalake service, currently supports `gravitino`.                                                                                                                                                                                                                                                              |
| recursive_file_scan        | boolean | no       | true                | Whether to scan subdirectories recursively. If `false`, subdirectories will be ignored.                                                                                                                                                                                                                                             |
| sort_files_by_modification_time | boolean | no       | false               | Sort files by modification time in descending order. Enable this when reading evolving schemas to ensure schema inference uses the latest file.                                                                                                               |

### file_format_type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`

If you assign file type to `markdown`, SeaTunnel can parse markdown files and extract structured data.
The markdown parser extracts various elements including headings, paragraphs, lists, code blocks, tables, and more.
Each extracted element is converted to a document-element row with the following schema:
- `element_id`: Unique identifier for the element
- `element_type`: Type of the element (Heading, Paragraph, ListItem, etc.)
- `heading_level`: Level of heading (1-6, null for non-heading elements)
- `text`: Text content of the element
- `page_number`: Page number (default: 1)
- `position_index`: Position index within the document
- `parent_id`: ID of the parent element
- `child_ids`: Comma-separated list of child element IDs

When either `markdown_rag_metadata_enabled` or `pdf_rag_metadata_enabled` is set to `true`, SeaTunnel appends the following RAG metadata fields after `child_ids` for the corresponding file type:
- `source_uri`: Source file path or URI
- `document_id`: Stable document identifier derived from `source_uri`
- `chunk_id`: Stable chunk identifier derived from document identity, chunk order, and content hash
- `chunk_index`: One-based chunk order in the parsed document
- `content_hash`: SHA-256 hash of the emitted `text` value

When this option is enabled for bounded Markdown file sources, the source enumerator assigns each whole-file split by the same `document_id` hash so all rows derived from one document stay in the same source route bucket. The default round-robin split assignment is unchanged when the option is disabled.

The option defaults to `false`, so the original Markdown schema is unchanged unless you enable it.

When `markdown_rag_metadata_enabled=true`, each Markdown row also carries four logical Knowledge Sync metadata values in row options, and the source declares the same keys in its metadata schema:

- `SourceUri`: a credential-free logical source path or URI
- `DocumentId`: `doc_` plus the lowercase SHA-256 of the UTF-8 logical `SourceUri`
- `DocumentHash`: lowercase SHA-256 of the exact source bytes read before UTF-8 decoding
- `ChunkHash`: lowercase SHA-256 of the immediate Markdown row's UTF-8 `text` (null is treated as an empty string); this equals physical `content_hash`

Local paths and valid `file:` URIs keep the existing local-path normalization. For hierarchical remote URIs, logical `SourceUri` preserves the scheme, host, explicit port, and path while removing user info, the complete query, and the fragment. Scheme and host are lowercased. Resources whose identity exists only in a query must use a stable, non-sensitive path.

The five physical RAG fields and all existing formulas and routing behavior remain unchanged. Consequently, signed or credential-bearing remote URIs can have different logical and physical `document_id` values. Project logical `SourceUri` and `DocumentId` to non-conflicting aliases such as `ks_source_uri` and `ks_document_id` with the [Metadata transform](../../transforms/metadata.md).

Logical `ChunkHash` describes only the immediate Markdown output row. After a transform changes text or expands one row into multiple chunks, recompute the final `ChunkHash`, `ChunkId`, and `ChunkIndex` before a lifecycle sink. This bridge does not implement incremental comparison, writer affinity, stale-chunk deletion, or tombstones.

Note: Markdown format only supports reading, not writing.

If you assign file type to `pdf`, SeaTunnel can parse PDF files and extract structured document elements.
PDF uses the same document-element row schema described above.
For PDF input, enable `pdf_rag_metadata_enabled` to append the RAG metadata fields described above.

The main PDF-specific behaviors are:

- **With outline**: Extracts `heading`, `paragraph`, `image`, and `link` elements. Headings are derived from the outline structure, and elements are organized into a parent-child hierarchy reflecting the document's logical structure.
- **Without outline**: Extracts only `paragraph` and `image` elements in a flat structure without hierarchy.
- `element_type` values for PDF are `heading`, `paragraph`, `image`, and `link`.

Note: Only single-column (top-to-bottom) PDF layouts are supported. Multi-column layouts (e.g., side-by-side two-column documents) are not supported and may produce incorrect text ordering.

:::caution

For security reasons (XXE hardening), XML files (`file_format_type = xml`) containing a `<!DOCTYPE ...>` declaration — including benign declarations that only define internal, non-external entities — are rejected with a `FILE_READ_FAILED` error. There is no configuration option to restore the previous, less secure behavior. If your XML files are exported by a tool that emits a `DOCTYPE` header, remove it or pre-process the file before ingesting it with SeaTunnel.

:::

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to read. This param will be parsed by `Charset.forName(encoding)`.

### binary_chunk_size [int]

Only used when file_format_type is binary.

The chunk size (in bytes) for reading binary files. Default is 1024 bytes. Larger values may improve performance for large files but use more memory.

### binary_complete_file_mode [boolean]

Only used when file_format_type is binary.

Whether to read the complete file as a single chunk instead of splitting into chunks. When enabled, the entire file content will be read into memory at once. Default is false.

### quote_char [string]

A single character that encloses CSV fields, allowing fields with commas, line breaks, or quotes to be read correctly.

### escape_char [string]

A single character that allows the quote or other special characters to appear inside a CSV field without ending the field.

### recursive_file_scan [boolean]

Whether to scan subdirectories recursively.
If `false`, subdirectories will be ignored.

### sort_files_by_modification_time [boolean]

Whether to sort files by modification time in descending order. Default is `false`.

When enabled, files will be sorted by their modification time (newest first). This is useful when:
- Reading files with evolving schemas and you want schema inference to use the latest file
- You need to process files in chronological order

### file_filter_pattern [string]

Filter pattern, which used for filtering files.  If you only want to filter based on file names, simply write the regular file names; If you want to filter based on the file directory at the same time, the expression needs to start with `path`.

The pattern follows standard regular expressions. For details, please refer to https://en.wikipedia.org/wiki/Regular_expression.
There are some examples.

If the `path` is `/data/seatunnel`, and the file structure example is:
```
/data/seatunnel/20241001/report.txt
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
/data/seatunnel/20241012/logo.png
```
Matching Rules Example:

**Example 1**: *Match all .txt files*，Regular Expression:
```
.*.txt
```
The result of this example matching is:
```
/data/seatunnel/20241001/report.txt
```
**Example 2**: *Match all file starting with abc*，Regular Expression:
```
abc.*
```
The result of this example matching is:
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**Example 3**: *Match all files starting with abc in folder 20241007，And the fourth character is either h or g*, the Regular Expression:
```
/data/seatunnel/20241007/abc[h,g].*
```
The result of this example matching is:
```
/data/seatunnel/20241007/abch202410.csv
```
**Example 4**: *Match third level folders starting with 202410 and files ending with .csv*, the Regular Expression:
```
/data/seatunnel/202410\d*/.*.csv
```
The result of this example matching is:
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### schema [config]

Only need to be configured when the file_format_type are text, json, excel, xml or csv ( Or other format we can't read the schema from metadata).

#### fields [Config]

The schema of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

#### metadata_table_id [string]

The table identifier in the metadata service to fetch table schema. For Gravitino, the format should be `{catalog}.{database}.{table}`, such as `mysql-catalog.test_db.users`.

When specified, the connector will fetch table schema from the external metadata service instead of using manual `columns` definition.

> When using Gravitino as the metadata source, the column types from Gravitino will be automatically converted to SeaTunnel data types. For detailed type mapping information, please refer to [Gravitino Type Mapping](../../introduction/concepts/gravitino-type-mapping.md).

For more information, please refer to [Metadata SPI](../../introduction/concepts/metadata-spi.md).

## Continuous Discovery

`discovery_mode=continuous` keeps a streaming job running and polls OSS for new or changed objects. This mode uses the existing file comparison path; it does not consume OSS event notifications and does not emit object delete events or changelog rows.

Continuous discovery currently requires `file_format_type="binary"` and `sync_mode="update"`. Set `target_path` to the same base path used by the sink so the source can skip unchanged objects. The default `discovery_mode="once"` preserves the existing bounded-source behavior.

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

## How to Create a Oss Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from Oss and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to Oss
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

# Console printing of the read Oss data
sink {
  Console {
  }
}
```

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to Oss
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

# Console printing of the read Oss data
sink {
  Console {
  }
}
```

### Multiple Table

No need to config schema file type, eg: `orc`.

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

Need config schema file type, eg: `json`

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

### Filter File

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
    // file example abcD2024.csv
    file_filter_pattern = "abc[DX]*.*"
    // file filter by modified date between 20240101 and 20240105(not include), actually 20240104 is end date
    file_filter_modified_start = "2024-01-01 00:00:00"
    file_filter_modified_end = "2024-01-05 00:00:00"
  }
}

sink {
  Console {
  }
}
```

## Changelog

<ChangeLog />
