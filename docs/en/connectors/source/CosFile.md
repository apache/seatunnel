import ChangeLog from '../changelog/connector-file-cos.md';

# CosFile

> Cos file source connector

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

Read data from Tencent Cloud COS file system.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

To use this connector you need put hadoop-cos-{hadoop.version}-{version}.jar and cos_api-bundle-{version}.jar in ${SEATUNNEL_HOME}/lib dir, download: [Hadoop-Cos-release](https://github.com/tencentyun/hadoop-cos/releases). It only supports hadoop version 2.6.5+ and version 8.0.2+.

:::

## Options

| name                       | type    | required | default value               |
|----------------------------|---------|----------|-----------------------------|
| path                       | string  | yes      | -                           |
| file_format_type           | string  | yes      | -                           |
| bucket                     | string  | yes      | -                           |
| secret_id                  | string  | yes      | -                           |
| secret_key                 | string  | yes      | -                           |
| region                     | string  | yes      | -                           |
| read_columns               | list    | no       | -                           |
| delimiter/field_delimiter  | string  | no       | \001 for text and , for csv |
| row_delimiter              | string  | no       | \n                          |
| parse_partition_from_path  | boolean | no       | true                        |
| skip_header_row_number     | long    | no       | 0                           |
| date_format                | string  | no       | yyyy-MM-dd                  |
| datetime_format            | string  | no       | yyyy-MM-dd HH:mm:ss         |
| time_format                | string  | no       | HH:mm:ss                    |
| schema                     | config  | no       | -                           |
| sheet_name                 | string  | no       | -                           |
| excel_engine               | string  | no       | POI                         |
| poi_excel_max_file_size    | long    | no       | 52428800                    |
| xml_row_tag                | string  | no       | -                           |
| xml_use_attr_format        | boolean | no       | -                           |
| csv_use_header_line        | boolean | no       | false                       |
| file_filter_pattern        | string  | no       | -                           |
| filename_extension         | string  | no       | -                           |
| compress_codec             | string  | no       | none                        |
| archive_compress_codec     | string  | no       | none                        |
| encoding                   | string  | no       | UTF-8                       |
| binary_chunk_size          | int     | no       | 1024                        |
| binary_complete_file_mode  | boolean | no       | false                       |
| common-options             |         | no       | -                           |
| file_filter_modified_start | string  | no       | -                           | 
| file_filter_modified_end   | string  | no       | -                           | 
| quote_char                 | string  | no       | "                           |
| escape_char                | string  | no       | -                           |
| recursive_file_scan        | boolean | no       | true                        |
| sort_files_by_modification_time | boolean | no       | false                       |

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

### bucket [string]

The bucket address of COS file system, for example: `cosn://seatunnel-test`

### secret_id [string]

The secret id of Cos file system. Issue this from the [Tencent Cloud CAM console](https://console.cloud.tencent.com/cam/capi) (SecretId field). For production jobs, prefer a CAM role with a scoped policy (e.g. `QcloudCOSReadOnlyAccess`) and use role-based temporary keys via STS instead of long-lived keys.

### secret_key [string]

The secret key of Cos file system. The SecretKey that pairs with `secret_id`. See `secret_id` for the recommended STS-based replacement.

### region [string]

The region of cos file system. Use a region that matches your bucket's actual location (for example `ap-guangzhou`, `ap-shanghai`, `ap-chengdu`). Bucket access from a different region still works but incurs cross-region transfer cost and latency.

### read_columns [list]

The read column list of the data source, user can use it to implement field projection.

### delimiter/field_delimiter [string]

**delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead.

Only need to be configured when file_format is text.

Field delimiter, used to tell connector how to slice and dice fields

default `\001`, the same as hive's default delimiter

### row_delimiter [string]

Only need to be configured when file_format is text

Row delimiter, used to tell connector how to slice and dice rows

default `\n`

### parse_partition_from_path [boolean]

Control whether parse the partition keys and values from file path

For example if you read a file from path `cosn://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

Every record data from file will be added these two fields:

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

Tips: **Do not define partition fields in schema option**

### skip_header_row_number [long]

Skip the first few lines, but only for the txt and csv.

For example, set like following:

`skip_header_row_number = 2`

then SeaTunnel will skip the first 2 lines from source files

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

### schema [config]

Only need to be configured when the file_format_type are text, json, excel, xml or csv ( Or other format we can't read the schema from metadata).

#### fields [Config]

The schema of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

### sheet_name [string]

Only need to be configured when file_format is excel.

Reader the sheet of the workbook.

### excel_engine [string]

Only used when `file_format` is excel.

Supported engines are `POI` and `EasyExcel`. The default value is `POI`.

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

### binary_chunk_size [int]

Only used when file_format_type is binary.

The chunk size (in bytes) for reading binary files. Default is 1024 bytes. Larger values may improve performance for large files but use more memory.

### binary_complete_file_mode [boolean]

Only used when file_format_type is binary.

Whether to read the complete file as a single chunk instead of splitting into chunks. When enabled, the entire file content will be read into memory at once. Default is false.

### file_filter_modified_start [string]

File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`.

### file_filter_modified_end [string]

File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`.

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

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

```hocon

  CosFile {
    path = "/seatunnel/orc"
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    file_format_type = "orc"
  }

```

```hocon

  CosFile {
    path = "/seatunnel/json"
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    file_format_type = "json"
    schema {
      fields {
        id = int 
        name = string
      }
    }
  }

```

### Transfer Binary File

```hocon

env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  CosFile {
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
    binary_chunk_size = 2048
    binary_complete_file_mode = false
  }
}
sink {
  // you can transfer local file to s3/hdfs/oss etc.
  CosFile {
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
    path = "/seatunnel/read/binary2/"
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
  CosFile {
    bucket = "cosn://seatunnel-test-1259587829"
    secret_id = "xxxxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxxxx"
    region = "ap-chengdu"
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

## Changelog

<ChangeLog />
