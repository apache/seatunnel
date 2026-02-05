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

- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
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
| sync_mode                  | string  | no       | full                                 |
| target_path                | string  | no       | -                                    |
| target_hadoop_conf         | map     | no       | -                                    |
| update_strategy            | string  | no       | distcp                               |
| compare_mode               | string  | no       | len_mtime                            |
| common-options             |         | no       | -                                    |
| tables_configs             | list    | no       | used to define a multiple table task |
| file_filter_modified_start | string  | no       | -                                    |
| file_filter_modified_end   | string  | no       | -                                    | 
| enable_file_split          | boolean | no       | false                                | 
| file_split_size            | long    | no       | 134217728                            |
| quote_char                 | string  | no       | "                                    |
| escape_char                | string  | no       | -                                    |
### path [string]

The source file path.

### file_format_type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown`

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
Each element is converted to a row with the following schema:
- `element_id`: Unique identifier for the element
- `element_type`: Type of the element (Heading, Paragraph, ListItem, etc.)
- `heading_level`: Level of heading (1-6, null for non-heading elements)
- `text`: Text content of the element
- `page_number`: Page number (default: 1)
- `position_index`: Position index within the document
- `parent_id`: ID of the parent element
- `child_ids`: Comma-separated list of child element IDs

Note: Markdown format only supports reading, not writing.

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

### sheet_name [string]

Only need to be configured when file_format is excel.

Reader the sheet of the workbook.

### excel_engine [string]

Only need to be configured when file_format is excel.

supported as the following file types:
`POI` `EasyExcel`

The default excel reading engine is POI, but POI can easily cause memory overflow when reading Excel with more than 65,000 rows, so you can switch to EasyExcel as the reading engine.


### xml_row_tag [string]

Only need to be configured when file_format is xml.

Specifies the tag name of the data rows within the XML file.

### xml_use_attr_format [boolean]

Only need to be configured when file_format is xml.

Specifies Whether to process data using the tag attribute format.

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
