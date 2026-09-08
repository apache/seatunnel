import ChangeLog from '../changelog/connector-file-local.md';

# LocalFile

> Local file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
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

## Description

Read data from local file system.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

:::

## Options

| name                       | type    | required | default value                        |
|----------------------------|---------|----------|--------------------------------------|
| path                       | string  | yes      | -                                    |
| file_format_type           | string  | yes      | -                                    |
| read_columns               | list    | no       | -                                    |
| delimiter/field_delimiter  | string  | no       | \001 for text and , for csv          |
| row_delimiter              | string  | no       | \n                                   |
| parse_partition_from_path  | boolean | no       | true                                 |
| date_format                | string  | no       | yyyy-MM-dd                           |
| datetime_format            | string  | no       | yyyy-MM-dd HH:mm:ss                  |
| time_format                | string  | no       | HH:mm:ss                             |
| skip_header_row_number     | long    | no       | 0                                    |
| schema                     | config  | no       | -                                    |
| sheet_name                 | string  | no       | -                                    |
| excel_engine               | string  | no       | POI                                  |
| poi_excel_max_file_size    | long    | no       | 52428800                             |
| xml_row_tag                | string  | no       | -                                    |
| xml_use_attr_format        | boolean | no       | -                                    |
| csv_use_header_line        | boolean | no       | false                                |
| file_filter_pattern        | string  | no       | -                                    |
| filename_extension         | string  | no       | -                                    |
| compress_codec             | string  | no       | none                                 |
| archive_compress_codec     | string  | no       | none                                 |
| encoding                   | string  | no       | UTF-8                                |
| null_format                | string  | no       | -                                    |
| binary_chunk_size          | int     | no       | 1024                                 |
| binary_complete_file_mode  | boolean | no       | false                                |
| discovery_mode             | string  | no       | once                                 |
| scan_interval              | string  | no       | 10S |
| start_mode                 | string  | no       | earliest                             |
| sync_mode                  | string  | no       | full                                 |
| target_path                | string  | no       | -                                    |
| target_hadoop_conf         | map     | no       | -                                    |
| update_strategy            | string  | no       | distcp                               |
| compare_mode               | string  | no       | len_mtime                            |
| update_compare_parallelism | int     | no       | 8                                    |
| update_compare_bulk_threshold | int  | no       | 0                                    |
| post_sync_action           | string  | no       | none                                 |
| backup_path                | string  | no       | -                                    |
| retention_max_age          | string  | no       | -                                    |
| retention_check_interval   | string  | no       | 1H                                   |
| common-options             |         | no       | -                                    |
| tables_configs             | list    | no       | used to define a multiple table task |
| file_filter_modified_start | string  | no       | -                                    |
| file_filter_modified_end   | string  | no       | -                                    | 
| enable_file_split          | boolean | no       | false                                | 
| file_split_size            | long    | no       | 134217728                            |
| quote_char                 | string  | no       | "                                    |
| escape_char                | string  | no       | -                                    |
| metalake_type              | string  | no       | gravitino                            |
| recursive_file_scan        | boolean | no       | true                                 |
| sort_files_by_modification_time | boolean | no       | false                                |

### path [string]

The source file path.

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
at the same time. You can find the specific usage in the example below.

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

### read_columns [list]

The read column list of the data source, user can use it to implement field projection.

### delimiter/field_delimiter [string]

**delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead.

Only need to be configured when file_format is text.

Field delimiter, used to tell connector how to slice and dice fields.

default `\001`, the same as hive's default delimiter

### row_delimiter [string]

Only need to be configured when file_format is text

Row delimiter, used to tell connector how to slice and dice rows

default `\n`

### parse_partition_from_path [boolean]

Control whether parse the partition keys and values from file path

For example if you read a file from path `file://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

Every record data from file will be added these two fields:

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

Tips: **Do not define partition fields in schema option**

### date_format [string]

Date type format, used to tell connector how to convert string to date, supported as the following formats:

`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`

default `yyyy-MM-dd`

### datetime_format [string]

Datetime type format, used to tell connector how to convert string to datetime, supported as the following formats:

`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`

default `yyyy-MM-dd HH:mm:ss`

### time_format [string]

Time type format, used to tell connector how to convert string to time, supported as the following formats:

`HH:mm:ss` `HH:mm:ss.SSS`

default `HH:mm:ss`

### skip_header_row_number [long]

Skip the first few lines, but only for the txt and csv.

For example, set like following:

`skip_header_row_number = 2`

then SeaTunnel will skip the first 2 lines from source files

### schema [config]

Only need to be configured when the file_format_type are text, json, excel, xml or csv ( Or other format we can't read the schema from metadata).

#### fields [Config]

The schema information of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

#### metadata_table_id [string]

The table identifier in the metadata service to fetch table schema. For Gravitino, the format should be `{catalog}.{database}.{table}`, such as `mysql-catalog.test_db.users`.

When specified, the connector will fetch table schema from the external metadata service instead of using manual `columns` definition.

> When using Gravitino as the metadata source, the column types from Gravitino will be automatically converted to SeaTunnel data types. For detailed type mapping information, please refer to [Gravitino Type Mapping](../../introduction/concepts/gravitino-type-mapping.md).

For more information, please refer to [Metadata SPI](../../introduction/concepts/metadata-spi.md).

### sheet_name [string]

Only need to be configured when file_format is excel.

Reader the sheet of the workbook.

### excel_engine [string]

Only need to be configured when file_format is excel.

supported as the following file types:
`POI` `EasyExcel`

The default Excel reading engine is POI. POI keeps the historical read behavior, including POI-specific formula and formatting handling, but it may use a lot of memory for large Excel files.

You can set `excel_engine = EasyExcel` to use streaming reads for large Excel files.

### poi_excel_max_file_size [long]

Only used when `file_format` is excel and `excel_engine` is POI.

The maximum Excel file size in bytes that the POI engine can read. The default value is `52428800` bytes (50 MB). When the file is larger than this limit, the connector fails fast and suggests using EasyExcel.


### xml_row_tag [string]

Only need to be configured when file_format is xml.

Specifies the tag name of the data rows within the XML file.

### xml_use_attr_format [boolean]

Only need to be configured when file_format is xml.

Specifies Whether to process data using the tag attribute format.

:::caution

For security reasons (XXE hardening), XML files (`file_format_type = xml`) containing a `<!DOCTYPE ...>` declaration — including benign declarations that only define internal, non-external entities — are rejected with a `FILE_READ_FAILED` error. There is no configuration option to restore the previous, less secure behavior. If your XML files are exported by a tool that emits a `DOCTYPE` header, remove it or pre-process the file before ingesting it with SeaTunnel.

:::

### csv_use_header_line [boolean]

Whether to use the header line to parse the file, only used when the file_format is `csv` and the file contains the header line that match RFC 4180

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

### filename_extension [string]

Filter filename extension, which used for filtering files with specific extension. Example: `csv` `.txt` `json` `.xml`.

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
|------------------------|--------------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

Note: gz compressed excel file needs to compress the original file or specify the file suffix, such as e2e.xls ->e2e_test.xls.gz

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to read. This param will be parsed by `Charset.forName(encoding)`.

### null_format [string]

Only used when file_format_type is text.
null_format to define which strings can be represented as null.

e.g: `\N`

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
- It is not recommended for massive small-file scenarios.

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

A positive value switches comparison to one directory listing when the candidate count under a target parent reaches the threshold. The default `0` disables automatic bulk listing and uses bounded point lookups, avoiding an unexpectedly expensive target directory scan. This behavior applies to all target filesystems. Source filters are applied while entries are listed to reduce peak metadata memory.

### post_sync_action [string]

Only used when `discovery_mode=continuous`. Supported values: `none` (default), `delete`, `backup`. In `discovery_mode=once`, setting `post_sync_action=delete` or `post_sync_action=backup` is rejected during config validation.

- `none`: default behavior, no source-side file operation.
- `delete`: delete processed source files after `notifyCheckpointComplete`; failed operations are retried on later checkpoints.
- `backup`: move processed source files to `backup_path` after `notifyCheckpointComplete`; failed operations are retried on later checkpoints.

Before `delete` or `backup`, SeaTunnel renames the source file to a staging/trash path first, then re-checks the file length and modification time. If the version differs after the rename, the file is restored so the next scan can re-discover the changed version.

**mtime granularity limitation**: The local filesystem on some platforms has 1-second mtime granularity. The act-then-verify approach narrows but cannot fully eliminate the race window if a same-second, same-length modification occurs. For maximum safety, ensure no concurrent writers are active during post-sync processing, or use `backup` instead of `delete` so the file is recoverable.

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

### file_filter_modified_start [string]

File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`.

### file_filter_modified_end [string]

File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`.

### enable_file_split [boolean]

Turn on the file splitting function, the default is false.It can be selected when the file type is csv, text, json, parquet and non-compressed format.

**Recommendations**
- Enable when reading a few large files and you want higher read parallelism.
- Disable when reading many small files, or when parallelism is low (splitting adds overhead).

**Limitations**
- Not supported for compressed files (`compress_codec` != `none`) or archive files (`archive_compress_codec` != `none`) — it will fall back to non-splitting.
- For `text`/`csv`/`json`, actual split size may be larger than `file_split_size` because the split end is aligned to the next `row_delimiter`.
- LocalFile uses Hadoop LocalFileSystem internally; no extra Hadoop configuration is required.

### file_split_size [long]

File split size, which can be filled in when the enable_file_split parameter is true. The unit is the number of bytes. The default value is the number of bytes of 128MB, which is 134217728.

**Tuning**
- Start with the default (128MB). Decrease it if parallelism is under-utilized; increase it if the number of splits is too large.
- Rough rule: `file_split_size ≈ file_size / desired_parallelism`.

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

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details

### tables_configs

Used to define a multiple table task, when you have multiple tables to read, you can use this option to define multiple tables.

## Example

### One Table

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

For json, text or csv file format with `encoding`

```hocon

LocalFile {
    path = "/tmp/hive/warehouse/test2"
    file_format_type = "text"
    encoding = "gbk"
}

```

### Multiple Table

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

### Read PDF File

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

For best results, use PDF files that contain an outline (bookmarks/table of contents). This enables the parser to extract headings with hierarchy information.

### Transfer Binary File

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
  // you can transfer local file to s3/hdfs/oss etc.
  LocalFile {
    path = "/seatunnel/read/binary2/"
    file_format_type = "binary"
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

### Continuous Discovery (discovery_mode=continuous)

`discovery_mode=continuous` keeps the job running and periodically scans the path for new/changed files (long-running job, recommended to run with `job.mode="STREAMING"`).

**Note:** `discovery_mode=continuous` currently requires `sync_mode="update"` (binary-only) to avoid repeated transfers without keeping an unbounded "seen" state. `target_path` should align with the sink `path` on the same filesystem.

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

### Filter File

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
    // file example abcD2024.csv
    file_filter_pattern = "abc[DX]*.*"
  }
}

sink {
  Console {
  }
}
```

## Changelog

<ChangeLog />
