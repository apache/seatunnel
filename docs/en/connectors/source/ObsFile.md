import ChangeLog from '../changelog/connector-file-obs.md';

# ObsFile

> Obs file source connector

## Support those engines

> Spark
>
> Flink
>
> Seatunnel Zeta

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
  - [x] markdown
  - [x] pdf

## Description

Read data from huawei cloud obs file system.

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to OBS and this connector need some hadoop dependencies.
It only supports hadoop version **2.9.X+**.

## Required Jar List

|        jar         |     supported versions      | maven                                                                                                  |
|--------------------|-----------------------------|--------------------------------------------------------------------------------------------------------|
| hadoop-huaweicloud | support version >= 3.1.1.29 | [Download](https://repo.huaweicloud.com/artifactory/sdk_public/org/apache/hadoop/hadoop-huaweicloud/)  |
| esdk-obs-java      | support version >= 3.19.7.3 | [Download](https://repo.huaweicloud.com/artifactory/sdk_public/com/huawei/storage/esdk-obs-java/)      |
| okhttp             | support version >= 3.11.0   | [Download](https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/)                                |
| okio               | support version >= 1.14.0   | [Download](https://repo1.maven.org/maven2/com/squareup/okio/okio/)                                     |

> Please download the support list corresponding to 'Maven' and copy them to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory.
>
> And copy all jars to $SEATUNNEL_HOME/lib/

## Options

| name                       | type    | required | default             | description                                                                                                                                                                          |
|----------------------------|---------|----------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                       | string  | yes      | -                   | The target dir path                                                                                                                                                                  |
| file_format_type           | string  | yes      | -                   | File type.[Tips](#file_format_type)                                                                                                                                                  |
| bucket                     | string  | yes      | -                   | The bucket address of obs file system, for example: `obs://obs-bucket-name`                                                                                                          |
| access_key                 | string  | yes      | -                   | The access key of obs file system                                                                                                                                                    |
| access_secret              | string  | yes      | -                   | The access secret of obs file system                                                                                                                                                 |
| endpoint                   | string  | yes      | -                   | The endpoint of obs file system                                                                                                                                                      |
| read_columns               | list    | no       | -                   | The read column list of the data source, user can use it to implement field projection.[Tips](#read_columns)                                                                         |
| delimiter                  | string  | no       | \001                | Field delimiter, used to tell connector how to slice and dice fields when reading text files                                                                                         |
| row_delimiter              | string  | no       | \n                  | Row delimiter, used to tell connector how to slice and dice rows when reading text files. Default is `\n` for text files.                                                            |
| parse_partition_from_path  | boolean | no       | true                | Control whether parse the partition keys and values from file path. [Tips](#parse_partition_from_path)                                                                               |
| skip_header_row_number     | long    | no       | 0                   | Skip the first few lines, but only for the txt and csv.                                                                                                                              |
| date_format                | string  | no       | yyyy-MM-dd          | Date type format, used to tell the connector how to convert string to date.[Tips](#date_format)                                                                                      |
| datetime_format            | string  | no       | yyyy-MM-dd HH:mm:ss | Datetime type format, used to tell the connector how to convert string to datetime.[Tips](#datetime_format)                                                                          |
| time_format                | string  | no       | HH:mm:ss            | Time type format, used to tell the connector how to convert string to time.[Tips](#time_format)                                                                                      |
| filename_extension         | string  | no       | -                   | Filter filename extension, which used for filtering files with specific extension. Example: `csv` `.txt` `json` `.xml`.                                                              |
| schema                     | config  | no       | -                   | [Tips](#schema)                                                                                                                                                                      |
| common-options             |         | no       | -                   | [Tips](#common_options)                                                                                                                                                              |
| sheet_name                 | string  | no       | -                   | Reader the sheet of the workbook,Only used when file_format is excel.                                                                                                                |
| excel_engine               | string  | no       | POI                 | Only used when `file_format` is excel. Supported engines are `POI` and `EasyExcel`.                                                                                                                                                                |
| poi_excel_max_file_size    | long    | no       | 52428800            | Only used when `file_format` is excel and `excel_engine` is POI. The maximum Excel file size in bytes that the POI engine can read (default 50 MB).                                                                                                |
| file_filter_modified_start | string  | no       | -                   | File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`. |
| file_filter_modified_end   | string  | no       | -                   | File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`. |
| quote_char                 | string  | no       | "                   | A single character that encloses CSV fields, allowing fields with commas, line breaks, or quotes to be read correctly.                                                               |
| escape_char                | string  | no       | -                   | A single character that allows the quote or other special characters to appear inside a CSV field without ending the field.                                                          |
| recursive_file_scan        | boolean | no       | true                | Whether to scan subdirectories recursively. If `false`, subdirectories will be ignored.                                                                                              |
| sort_files_by_modification_time | boolean | no       | false               | Sort files by modification time in descending order. Enable this when reading evolving schemas to ensure schema inference uses the latest file.                                                                                                               |

### Tips

#### <span id="parse_partition_from_path"> parse_partition_from_path </span>

> Control whether parse the partition keys and values from file path
>
> For example if you read a file from path `obs://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`
>
> Every record data from the file will be added these two fields:

|     name      | age |
|---------------|-----|
| tyrantlucifer | 26  |

> Do not define partition fields in schema option

#### <span id="date_format"> date_format </span>

> Date type format, used to tell the connector how to convert string to date, supported as the following formats:
>
> `yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`
>
> default `yyyy-MM-dd`

### <span id="datetime_format"> datetime_format </span>

> Datetime type format, used to tell the connector how to convert string to datetime, supported as the following formats:
>
> `yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`
>
> default `yyyy-MM-dd HH:mm:ss`

### <span id="time_format"> time_format </span>

> Time type format, used to tell the connector how to convert string to time, supported as the following formats:
>
> `HH:mm:ss` `HH:mm:ss.SSS`
>
> default `HH:mm:ss`

### <span id="skip_header_row_number"> skip_header_row_number </span>

> Skip the first few lines, but only for the txt and csv.
>
> For example, set like following:
>
> `skip_header_row_number = 2`
>
> Then Seatunnel will skip the first 2 lines from source files

### <span id="file_format_type"> file_format_type </span>

> File type, supported as the following file types:
>
> `text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`
>
> If you assign file type to `json`, you should also assign schema option to tell the connector how to parse data to the row you want.
>
> For example, upstream data is the following:
>
> ```json
> {"code": 200, "data": "get success", "success": true}
> ```

> You can also save multiple pieces of data in one file and split them by one newline:

```json lines

{"code":  200, "data":  "get success", "success":  true}
{"code":  300, "data":  "get failed", "success":  false}

```

> you should assign schema as the following:

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

> connector will generate data as the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

> If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.
>
> If you assign file type to `text` `csv`, you can choose to specify the schema information or not.
>
> For example, upstream data is the following:

```text

tyrantlucifer#26#male

```

> If you do not assign data schema connector will treat the upstream data as the following:

|        content        |
|-----------------------|
| tyrantlucifer#26#male |

> If you assign data schema, you should also assign the option `delimiter` too except CSV file type
>
> you should assign schema and delimiter as the following:

```hocon

delimiter = "#"
schema {
    fields {
        name = string
        age = int
        gender = string 
    }
}

```

> connector will generate data as the following:

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

> If you assign file type to `markdown`, SeaTunnel can parse markdown files and extract structured data.
> The markdown parser extracts various elements including headings, paragraphs, lists, code blocks, tables, and more.
> Each extracted element is converted to a document-element row with the following schema:
> - `element_id`: Unique identifier for the element
> - `element_type`: Type of the element (Heading, Paragraph, ListItem, etc.)
> - `heading_level`: Level of heading (1-6, null for non-heading elements)
> - `text`: Text content of the element
> - `page_number`: Page number (default: 1)
> - `position_index`: Position index within the document
> - `parent_id`: ID of the parent element
> - `child_ids`: Comma-separated list of child element IDs
>
> When either `markdown_rag_metadata_enabled` or `pdf_rag_metadata_enabled` is set to `true`, SeaTunnel appends the following RAG metadata fields after `child_ids` for the corresponding file type:
> - `source_uri`: Source file path or URI
> - `document_id`: Stable document identifier derived from `source_uri`
> - `chunk_id`: Stable chunk identifier derived from document identity, chunk order, and content hash
> - `chunk_index`: One-based chunk order in the parsed document
> - `content_hash`: SHA-256 hash of the emitted `text` value
>
> When this option is enabled for bounded Markdown file sources, the source enumerator assigns each whole-file split by the same `document_id` hash so all rows derived from one document stay in the same source route bucket. The default round-robin split assignment is unchanged when the option is disabled.
>
> The option defaults to `false`, so the original Markdown schema is unchanged unless you enable it.
>
> When `markdown_rag_metadata_enabled=true`, each Markdown row also carries four logical Knowledge Sync metadata values in row options, and the source declares the same keys in its metadata schema:
>
> - `SourceUri`: a credential-free logical source path or URI
> - `DocumentId`: `doc_` plus the lowercase SHA-256 of the UTF-8 logical `SourceUri`
> - `DocumentHash`: lowercase SHA-256 of the exact source bytes read before UTF-8 decoding
> - `ChunkHash`: lowercase SHA-256 of the immediate Markdown row's UTF-8 `text` (null is treated as an empty string); this equals physical `content_hash`
>
> Local paths and valid `file:` URIs keep the existing local-path normalization. For hierarchical remote URIs, logical `SourceUri` preserves the scheme, host, explicit port, and path while removing user info, the complete query, and the fragment. Scheme and host are lowercased. Resources whose identity exists only in a query must use a stable, non-sensitive path.
>
> The five physical RAG fields and all existing formulas and routing behavior remain unchanged. Consequently, signed or credential-bearing remote URIs can have different logical and physical `document_id` values. Project logical `SourceUri` and `DocumentId` to non-conflicting aliases such as `ks_source_uri` and `ks_document_id` with the [Metadata transform](../../transforms/metadata.md).
>
> Logical `ChunkHash` describes only the immediate Markdown output row. After a transform changes text or expands one row into multiple chunks, recompute the final `ChunkHash`, `ChunkId`, and `ChunkIndex` before a lifecycle sink. This bridge does not implement incremental comparison, writer affinity, stale-chunk deletion, or tombstones.
>
> Note: Markdown format only supports reading, not writing.
>
> If you assign file type to `pdf`, SeaTunnel can parse PDF files and extract structured document elements.
> PDF uses the same document-element row schema described above.
> For PDF input, enable `pdf_rag_metadata_enabled` to append the RAG metadata fields described above.
>
> The main PDF-specific behaviors are:
>
> - **With outline**: Extracts `heading`, `paragraph`, `image`, and `link` elements. Headings are derived from the outline structure, and elements are organized into a parent-child hierarchy reflecting the document's logical structure.
> - **Without outline**: Extracts only `paragraph` and `image` elements in a flat structure without hierarchy.
> - `element_type` values for PDF are `heading`, `paragraph`, `image`, and `link`.
>
> Note: Only single-column (top-to-bottom) PDF layouts are supported. Multi-column layouts (e.g., side-by-side two-column documents) are not supported and may produce incorrect text ordering.

#### <span id="schema"> schema  </span>

##### fields

> The schema of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

#### <span id="schema"> read_columns </span>

> The read column list of the data source, user can use it to implement field projection.
>
> The file type supported column projection as the following shown:

- text
- json
- csv
- orc
- parquet
- excel

> If the user wants to use this feature when reading `text` `json` `csv` files, the schema option must be configured

#### <span id="common_options "> common options </span>

> Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

#### <span id="sort_files_by_modification_time"> sort_files_by_modification_time </span>

> Whether to sort files by modification time in descending order. Default is `false`.
>
> When enabled, files will be sorted by their modification time (newest first). This is useful when:
> - Reading files with evolving schemas and you want schema inference to use the latest file
> - You need to process files in chronological order

## Task Example

### text file

> For text file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/text"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "text"
  }

```

### parquet file

> For parquet file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/parquet"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "parquet"
  }

```

### orc file

> For orc file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/orc"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "orc"
  }

```

### json file

> For json file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/json"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "json"
  }

```

### excel file

> For excel file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/excel"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "excel"
  }

```

### csv file

> For csv file format simple config

```hocon

  ObsFile {
    path = "/seatunnel/csv"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    file_format_type = "csv"
    delimiter = ","
  }

```

### Reading with Temporary Security Credentials (OBS STS)

For production jobs that need scoped, short-lived access, generate temporary AK/SK via [OBS STS](https://support.huaweicloud.com/intl/en-us/api-obs/obs_04_0081.html) and pass them through `hadoop_obs_properties`. The temporary credentials can carry a fine-grained custom policy that limits access to a specific bucket prefix.

```hocon
source {
  ObsFile {
    path = "/staging/prefix"
    bucket = "obs://target-bucket"
    endpoint = "obs.ap-southeast-1.myhuaweicloud.com"
    hadoop_obs_properties = {
      "fs.obs.access.key"    = "<temp-access-key>"
      "fs.obs.secret.key"    = "<temp-secret-key>"
      "fs.obs.session.token" = "<temp-security-token>"
    }
    file_format_type = "parquet"
  }
}
```

The provider jar must be on the runtime classpath of every node (`${SEATUNNEL_HOME}/lib`). Avoid long-lived AK/SK in job files; prefer STS-issued temporary credentials or an ECS Agency when running inside Huawei Cloud.

## Changelog

<ChangeLog />
