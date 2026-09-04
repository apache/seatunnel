import ChangeLog from '../changelog/connector-file-sftp.md';

# SftpFile

> Sftp file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] file format type
  - [x] text
  - [x] csv
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown
  - [x] pdf

## Description

Read data from sftp file server.

## Supported DataSource Info

In order to use the SftpFile connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                       Dependency                                        |
|------------|--------------------|-----------------------------------------------------------------------------------------|
| SftpFile   | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-file-sftp) |

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to Sftp and this connector need some hadoop dependencies.
It only supports hadoop version **2.9.X+**.

:::

## Data Type Mapping

The File does not have a specific type list, and we can indicate which SeaTunnel data type the corresponding data needs to be converted to by specifying the Schema in the config.

| SeaTunnel Data type |
|---------------------|
| STRING              |
| SHORT               |
| INT                 |
| BIGINT              |
| BOOLEAN             |
| DOUBLE              |
| DECIMAL             |
| FLOAT               |
| DATE                |
| TIME                |
| TIMESTAMP           |
| BYTES               |
| ARRAY               |
| MAP                 |

## Source Options

| Name                       | Type    | Required | default value                 | Description                                                                                                                                                                                                                                                                                                                                                                     |
|----------------------------|---------|----------|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                       | String  | Yes      | -                             | The target sftp host is required                                                                                                                                                                                                                                                                                                                                                |
| port                       | Int     | Yes      | -                             | The target sftp port is required                                                                                                                                                                                                                                                                                                                                                |
| user                       | String  | Yes      | -                             | The target sftp username is required                                                                                                                                                                                                                                                                                                                                            |
| password                   | String  | No       | -                             | The target sftp password. Required when `keyfile` is not set.                                                                                                                                                                                                                                                                                                                   |
| keyfile                    | String  | No       | -                             | The private key file path used for SFTP public key authentication.                                                                                                                                                                                                                                                                                                              |
| path                       | String  | Yes      | -                             | The source file path.                                                                                                                                                                                                                                                                                                                                                           |
| file_format_type           | String  | Yes      | -                             | Please check #file_format_type below. Supported file types: `text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`                                                                                                                                                                                                                                                                                                                                            |
| file_filter_pattern        | String  | No       | -                             | Filter pattern, which used for filtering files.                                                                                                                                                                                                                                                                                                                                 |
| filename_extension         | string  | no       | -                             | Filter filename extension, which used for filtering files with specific extension. Example: `csv` `.txt` `json` `.xml`.                                                                                                                                                                                                                                                         |
| delimiter/field_delimiter  | String  | No       | \001 for text and ',' for csv | **delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead. <br/> Field delimiter, used to tell connector how to slice and dice fields when reading text files. <br/> Default `\001`, the same as hive's default delimiter                                                                                                              |
| row_delimiter              | string  | no       | \n                            | Row delimiter, used to tell connector how to slice and dice rows when reading text files. <br/> Default `\n`                                                                                                                                                                                                                                                                    |
| parse_partition_from_path  | Boolean | No       | true                          | Control whether parse the partition keys and values from file path <br/> For example if you read a file from path `oss://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26` <br/> Every record data from file will be added these two fields: <br/>      name       age  <br/> tyrantlucifer  26   <br/> Tips: **Do not define partition fields in schema option** |
| date_format                | String  | No       | yyyy-MM-dd                    | Date type format, used to tell connector how to convert string to date, supported as the following formats: <br/> `yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd` <br/> default `yyyy-MM-dd`                                                                                                                                                                                             |
| datetime_format            | String  | No       | yyyy-MM-dd HH:mm:ss           | Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats: <br/> `yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss` <br/> default `yyyy-MM-dd HH:mm:ss`                                                                                                                                |
| time_format                | String  | No       | HH:mm:ss                      | Time type format, used to tell connector how to convert string to time, supported as the following formats: <br/> `HH:mm:ss` `HH:mm:ss.SSS` <br/> default `HH:mm:ss`                                                                                                                                                                                                            |
| skip_header_row_number     | Long    | No       | 0                             | Skip the first few lines, but only for the txt and csv. <br/> For example, set like following: <br/> `skip_header_row_number = 2` <br/> then SeaTunnel will skip the first 2 lines from source files                                                                                                                                                                            |
| read_columns               | list    | no       | -                             | The read column list of the data source, user can use it to implement field projection.                                                                                                                                                                                                                                                                                         |
| sheet_name                 | String  | No       | -                             | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                                                                                                                                                                                                           |
| excel_engine               | string  | no       | POI                           | Only used when `file_format` is excel. Supported engines are `POI` and `EasyExcel`.                                                                                                                                                                                                                                                                                            |
| poi_excel_max_file_size    | long    | no       | 52428800                      | Only used when `file_format` is excel and `excel_engine` is POI. The maximum Excel file size in bytes that the POI engine can read (default 50 MB).                                                                                                                                                                                                                            |
| xml_row_tag                | string  | no       | -                             | Specifies the tag name of the data rows within the XML file, only used when file_format is xml.                                                                                                                                                                                                                                                                                 |
| xml_use_attr_format        | boolean | no       | -                             | Specifies whether to process data using the tag attribute format, only used when file_format is xml.                                                                                                                                                                                                                                                                            |
| csv_use_header_line        | boolean | no       | false                         | Whether to use the header line to parse the file, only used when the file_format is `csv` and the file contains the header line that match RFC 4180                                                                                                                                                                                                                             |
| schema                     | Config  | No       | -                             | Please check #schema below                                                                                                                                                                                                                                                                                                                                                      |
| compress_codec             | String  | No       | None                          | The compress codec of files and the details that supported as the following shown: <br/> - txt: `lzo` `None` <br/> - json: `lzo` `None` <br/> - csv: `lzo` `None` <br/> - orc: `lzo` `snappy` `lz4` `zlib` `None` <br/> - parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `None` <br/> Tips: excel type does Not support any compression format                            |
| archive_compress_codec     | string  | no       | none                          |                                                                                                                                                                                                                                                                                                                                                                                 |
| encoding                   | string  | no       | UTF-8                         |                                                                                                                                                                                                                                                                                                                                                                                 |
| null_format                | string  | no       | -                             | Only used when file_format_type is text. null_format to define which strings can be represented as null. e.g: `\N`                                                                                                                                                                                                                                                              |
| binary_chunk_size          | int     | no       | 1024                          | Only used when file_format_type is binary. The chunk size (in bytes) for reading binary files. Default is 1024 bytes. Larger values may improve performance for large files but use more memory.                                                                                                                                                                                |
| binary_complete_file_mode  | boolean | no       | false                         | Only used when file_format_type is binary. Whether to read the complete file as a single chunk instead of splitting into chunks. When enabled, the entire file content will be read into memory at once. Default is false.                                                                                                                                                      |
| discovery_mode             | string  | no       | once                          | File discovery mode. Supported values: `once` (default), `continuous`. When `continuous`, the source keeps scanning the path and processes new/changed files at runtime (unbounded). In the current implementation, `continuous` requires `sync_mode=update` (binary only).                                                                 |
| scan_interval              | string  | no       | 10S | Only used when `discovery_mode=continuous`. Scan interval for periodic discovery, recommended shorthand format `10S`, `30S`; ISO-8601 format `PT10S`, `PT30S` is also supported.                                                                                                                                                                                                                                                                    |
| start_mode                 | string  | no       | earliest                      | Only used when `discovery_mode=continuous`. Supported values: `earliest` (default), `latest`.                                                                                                                                                                                                                                                                                 |
| sync_mode                  | string  | no       | full                          | File sync mode. Supported values: `full`, `update`. When `update`, the source compares files between source/target and only reads new/changed files (currently only supports `file_format_type=binary`).                                                                                                                               |
| target_path                | string  | no       | -                             | Only used when `sync_mode=update`. Target base path used for comparison (it should usually be the same as sink `path`).                                                                                                                                                                                                           |
| target_hadoop_conf         | map     | no       | -                             | Only used when `sync_mode=update`. Extra Hadoop configuration for target filesystem. You can set `fs.defaultFS` in this map to override target defaultFS.                                                                                                                                                                           |
| update_strategy            | string  | no       | distcp                        | Only used when `sync_mode=update`. Supported values: `distcp` (default), `strict`.                                                                                                                                                                                                                                               |
| compare_mode               | string  | no       | len_mtime                     | Only used when `sync_mode=update`. Supported values: `len_mtime` (default), `checksum` (only valid when `update_strategy=strict`).                                                                                                                                                                                              |
| update_compare_parallelism | int     | no       | 8                             | Maximum parallelism for sparse target metadata lookups. Valid range: `1-64`.                                                                                                                                                                                                                                                   |
| update_compare_bulk_threshold | int  | no       | 0                             | A positive value switches comparison to one directory listing when the candidate count under one target parent reaches the threshold. `0` disables automatic bulk listing.                                                                                                                                                     |
| post_sync_action           | string  | no       | none                          | Post-sync action in `discovery_mode=continuous`. Supported values: `none` (default), `delete`, `backup`.                                                                                                                                                                                                                        |
| backup_path                | string  | no       | -                             | Backup destination base path when `post_sync_action=backup`. It must not overlap with `path`.                                                                                                                                                                                                                                   |
| retention_max_age          | string  | no       | -                             | Optional retention age for SeaTunnel backup files in `backup_path`, only valid when `post_sync_action=backup`.                                                                                                                                                                                                                  |
| retention_check_interval   | string  | no       | 1H                            | Retention scan interval, only effective when `post_sync_action=backup` and `retention_max_age` is configured.                                                                                                                                                                                                                   |
| common-options             |         | No       | -                             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                                                                                                                                                              |
| file_filter_modified_start | string  | no       | -                             | File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                                                            |
| file_filter_modified_end   | string  | no       | -                             | File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                                                            |
| quote_char                 | string  | no       | "                             | A single character that encloses CSV fields, allowing fields with commas, line breaks, or quotes to be read correctly.                                                                                                                                                                                                                                                          |
| escape_char                | string  | no       | -                             | A single character that allows the quote or other special characters to appear inside a CSV field without ending the field.                                                                                                                                                                                                                                                     |
| metalake_type              | string  | no       | gravitino                    | The type of metalake service, currently supports `gravitino`.                                                                                                                                                                                                                                                                                                                                                              |
| recursive_file_scan        | boolean | no       | true                          | Whether to scan subdirectories recursively. If `false`, subdirectories will be ignored.                                                                                                                                                                                                                                                                                         |
| sort_files_by_modification_time | boolean | no       | false                       | Sort files by modification time in descending order. Enable this when reading evolving schemas to ensure schema inference uses the latest file.                                                                                                               |

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

### file_format_type [string]

File type, supported as the following file types:
`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`
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
If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.
If you assign file type to `text` `csv`, you can choose to specify the schema information or not.
For example, upstream data is the following:

```text
tyrantlucifer#26#male
```

If you do not assign data schema connector will treat the upstream data as the following:
|        content        |
|-----------------------|
| tyrantlucifer#26#male |
If you assign data schema, you should also assign the option `field_delimiter` too except CSV file type
you should assign schema and delimiter as the following:

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

If you assign file type to `binary`, SeaTunnel can synchronize files in any format,
such as compressed packages, pictures, etc. In short, any files can be synchronized to the target place.
Under this requirement, you need to ensure that the source and sink use `binary` format for file synchronization
at the same time.

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

### archive_compress_codec [string]

The compress codec of archive files and the details that supported as the following shown:

| archive_compress_codec | file_format        | archive_compress_suffix |
|--------------------|--------------------|---------------------|
| ZIP                | txt,json,excel,xml | .zip                |
| TAR                | txt,json,excel,xml | .tar                |
| TAR_GZ             | txt,json,excel,xml | .tar.gz             |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

Note: gz compressed excel file needs to compress the original file or specify the file suffix, such as e2e.xls ->e2e_test.xls.gz

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to read. This param will be parsed by `Charset.forName(encoding)`.

### binary_chunk_size [int]

Only used when file_format_type is binary.

The chunk size (in bytes) for reading binary files. Default is 1024 bytes. Larger values may improve performance for large files but use more memory.

### binary_complete_file_mode [boolean]

Only used when file_format_type is binary.

Whether to read the complete file as a single chunk instead of splitting into chunks. When enabled, the entire file content will be read into memory at once. Default is false.

### discovery_mode [string]

File discovery mode. Supported values: `once` (default), `continuous`.

- `once`: enumerate current files once and finish (bounded).
- `continuous`: keep scanning the path and processing new/changed files at runtime (unbounded).

In the current implementation, `discovery_mode=continuous` requires `sync_mode=update` (binary only) to avoid repeated transfers.

### scan_interval [string]

Only used when `discovery_mode=continuous`. Scan interval for periodic discovery; value must be greater than `0`. Recommended shorthand format `10S`, `30S` (case-insensitive, e.g. `10s`); ISO-8601 format `PT10S`, `PT30S` is also supported. Default is `10S`.

### start_mode [string]

Only used when `discovery_mode=continuous`. Supported values: `earliest` (default), `latest`.

- `earliest`: read existing files on startup.
- `latest`: only process files modified after the job starts.

### sync_mode [string]

File sync mode. Supported values: `full` (default), `update`.
When `update`, the source compares files between source/target and only reads new/changed files (currently only supports `file_format_type=binary`).

**Performance considerations**
- Update mode triggers an extra `getFileStatus` call on the target for each source file.
- For remote file systems (FTP/SFTP), this adds per-file network overhead. It is not recommended for massive small-file scenarios.

**Requirements / limitations**
- `target_path` should typically align with sink `path` (same filesystem and same relative path layout).
- When `update_strategy=distcp`, correctness depends on source/target clock synchronization.
- When `compare_mode=checksum`, filesystem checksum support is required. If checksum is unavailable, SeaTunnel falls back to content comparison (more expensive) and logs a warning.

Example:

```hocon
sync_mode = "update"
file_format_type = "binary"
target_path = "/path/to/your/sink/path"
update_strategy = "distcp"
compare_mode = "len_mtime"
```

### target_path [string]

Only used when `sync_mode=update`. Target base path used for comparison (it should usually be the same as sink `path`).

### target_hadoop_conf [map]

Only used when `sync_mode=update`. Extra Hadoop configuration for target filesystem. You can set `fs.defaultFS` in this map to override target defaultFS.

### update_strategy [string]

Only used when `sync_mode=update`. Supported values: `distcp` (default), `strict`.

### compare_mode [string]

Only used when `sync_mode=update`. Supported values: `len_mtime` (default), `checksum` (only valid when `update_strategy=strict`).

### update_compare_parallelism [int]

Maximum parallelism for sparse target metadata lookups in `sync_mode=update`. The default is `8`; valid values are `1` through `64`; values outside this range are rejected during configuration validation. The maximum number of submitted-but-incomplete lookups is eight times this value.

### update_compare_bulk_threshold [int]

A positive value switches comparison to one directory listing when the candidate count under a target parent reaches the threshold. The default `0` disables automatic bulk listing and uses bounded point lookups, avoiding an unexpectedly expensive target directory scan. This behavior applies to all target filesystems. Source filters are applied while entries are listed to reduce peak metadata memory. A flat SFTP directory must still return all entries because SFTP `READDIR` has no server-side modification-time filter.

### post_sync_action [string]

Only used when `discovery_mode=continuous`. Supported values: `none` (default), `delete`, `backup`. In `discovery_mode=once`, setting `post_sync_action=delete` or `post_sync_action=backup` is rejected during config validation.

- `none`: default behavior, no source-side file operation.
- `delete`: delete processed source files after `notifyCheckpointComplete`; failed operations are retried on later checkpoints.
- `backup`: move processed source files to `backup_path` after `notifyCheckpointComplete`; failed operations are retried on later checkpoints.

Before `delete` or `backup`, SeaTunnel renames the source file to a staging/trash path first, then re-checks the file length and modification time. If the version differs after the rename, the file is restored so the next scan can re-discover the changed version.

**mtime granularity limitation**: SFTP mtime resolution is typically 1 second. The act-then-verify approach narrows but cannot fully eliminate the race window if a same-second, same-length modification occurs. For maximum safety, ensure no concurrent writers are active during post-sync processing, or use `backup` instead of `delete` so the file is recoverable.

**Security note**: When using `post_sync_action=delete` or `backup` with SFTP, use a dedicated least-privilege account that only has DELETE/RENAME permission on the watched directory. This feature requires write/delete permissions that increase the blast radius of a credential compromise.

### backup_path [string]

Only used when `post_sync_action=backup`. Processed files are moved to this base path after checkpoint-complete commit, and destination file names include source version suffix to avoid overwrite collision. Phase-1 only supports backup on the same filesystem as `path` (same scheme and authority); cross-filesystem backup is rejected.

`backup_path` must not be the same as `path`, must not be under `path`, and `path` must not be under `backup_path`. Use a dedicated backup directory because retention only manages files created by SeaTunnel with the version suffix.

### retention_max_age [string]

Optional retention policy for `backup_path`. SeaTunnel backup files older than this age are cleaned up during checkpoint-complete retention scans.
Only valid when `post_sync_action=backup`.

Supported duration formats are shorthand values with `MS`, `S`, `M`, `H`, or `D` suffixes, such as `500MS`, `30S`, `10M`, `12H`, `7D`, and ISO-8601 durations such as `PT1H30M`.

Duration suffixes are case-insensitive: `MS` (milliseconds), `S` (seconds), `M` (minutes), `H` (hours), `D` (days). `M` always means minutes, never months. Invalid values (e.g., `PT7D`, `P1M`) fail config validation with an error.

### retention_check_interval [string]

Retention scan interval, default `1H`. Cleanup runs at most once per interval when `post_sync_action=backup` and `retention_max_age` is configured. Setting `retention_check_interval` without `retention_max_age` has no effect.

Duration suffixes are case-insensitive: `MS`, `S`, `M`, `H`, `D`. `M` always means minutes, never months. Invalid values fail config validation with an error.

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

### schema [config]

#### fields [Config]

The schema of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

#### metadata_table_id [string]

The table identifier in the metadata service to fetch table schema. For Gravitino, the format should be `{catalog}.{database}.{table}`, such as `mysql-catalog.test_db.users`.

When specified, the connector will fetch table schema from the external metadata service instead of using manual `columns` definition.

> When using Gravitino as the metadata source, the column types from Gravitino will be automatically converted to SeaTunnel data types. For detailed type mapping information, please refer to [Gravitino Type Mapping](../../introduction/concepts/gravitino-type-mapping.md).

For more information, please refer to [Metadata SPI](../../introduction/concepts/metadata-spi.md).

## How to Create a Sftp Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from sftp and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to sftp
source {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass
    path = "tmp/seatunnel/read/json"
    file_format_type = "json"
    plugin_output = "sftp"
    schema = {
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
}

# Console printing of the read sftp data
sink {
  Console {
    parallelism = 1
  }
}
```
### Multiple Table

```hocon

SftpFile {
  tables_configs = [
    {
      schema {
        table = "student"
        fields {
          name = string
          age = int
        }
      }
      path = "/tmp/seatunnel/sink/text"
      host = "192.168.31.48"
      port = 21
      user = tyrantlucifer
      password = tianchao
      file_format_type = "parquet"
    },
    {
      schema {
        table = "teacher"
        fields {
          name = string
          age = int
        }
      }
      path = "/tmp/seatunnel/sink/text"
      host = "192.168.31.48"
      port = 21
      user = tyrantlucifer
      password = tianchao
      file_format_type = "parquet"
    }
  ]
}

```

### Filter File

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass
    path = "tmp/seatunnel/read/json"
    file_format_type = "json"
    plugin_output = "sftp"
    // file example abcD2024.csv
    file_filter_pattern = "abc[DX]*.*"
  }
}

sink {
  Console {
  }
}
```

### Incremental Sync (sync_mode=update, binary)

`sync_mode=update` compares files between source and `target_path`, then only reads new/changed files.
In most cases, `target_path` should be aligned with sink `path` (same filesystem and same relative paths).

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass

    path = "tmp/seatunnel/update/src"
    file_format_type = "binary"

    sync_mode = "update"
    target_path = "tmp/seatunnel/update/dst"
    update_strategy = "distcp"
    compare_mode = "len_mtime"
  }
}

sink {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass

    path = "tmp/seatunnel/update/dst"
    tmp_path = "tmp/seatunnel/update/tmp"
    file_format_type = "binary"
  }
}
```

### Continuous Discovery (discovery_mode=continuous)

`discovery_mode=continuous` keeps the job running and periodically scans the path for new/changed files (long-running job, recommended to run with `job.mode="STREAMING"`).

**Note:** `discovery_mode=continuous` currently requires `sync_mode="update"` (binary-only) to avoid repeated transfers without keeping an unbounded "seen" state. `target_path` should align with the sink `path` on the same filesystem.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass

    path = "tmp/seatunnel/watch/src"
    file_format_type = "binary"

    discovery_mode = "continuous"
    scan_interval = "10S"
    start_mode = "latest"

    sync_mode = "update"
    target_path = "tmp/seatunnel/watch/dst"
    update_strategy = "distcp"
    compare_mode = "len_mtime"

    post_sync_action = "backup"
    backup_path = "tmp/seatunnel/watch/backup"
    retention_max_age = "7D"
    retention_check_interval = "1H"
  }
}

sink {
  SftpFile {
    host = "sftp"
    port = 22
    user = seatunnel
    password = pass

    path = "tmp/seatunnel/watch/dst"
    tmp_path = "tmp/seatunnel/watch/tmp"
    file_format_type = "binary"
  }
}
```
## Changelog

<ChangeLog />
