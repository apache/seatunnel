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
| password                   | String  | Yes      | -                             | The target sftp password is required                                                                                                                                                                                                                                                                                                                                            |
| path                       | String  | Yes      | -                             | The source file path.                                                                                                                                                                                                                                                                                                                                                           |
| file_format_type           | String  | Yes      | -                             | Please check #file_format_type below                                                                                                                                                                                                                                                                                                                                            |
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
| sync_mode                  | string  | no       | full                          | File sync mode. Supported values: `full`, `update`. When `update`, the source compares files between source/target and only reads new/changed files (currently only supports `file_format_type=binary`).                                                                                                                               |
| target_path                | string  | no       | -                             | Only used when `sync_mode=update`. Target base path used for comparison (it should usually be the same as sink `path`).                                                                                                                                                                                                           |
| target_hadoop_conf         | map     | no       | -                             | Only used when `sync_mode=update`. Extra Hadoop configuration for target filesystem. You can set `fs.defaultFS` in this map to override target defaultFS.                                                                                                                                                                           |
| update_strategy            | string  | no       | distcp                        | Only used when `sync_mode=update`. Supported values: `distcp` (default), `strict`.                                                                                                                                                                                                                                               |
| compare_mode               | string  | no       | len_mtime                     | Only used when `sync_mode=update`. Supported values: `len_mtime` (default), `checksum` (only valid when `update_strategy=strict`).                                                                                                                                                                                              |
| common-options             |         | No       | -                             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                                                                                                                                                              |
| file_filter_modified_start | string  | no       | -                             | File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                                                            |
| file_filter_modified_end   | string  | no       | -                             | File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`.                                                                                                                                                                                            |
| quote_char                 | string  | no       | "                             | A single character that encloses CSV fields, allowing fields with commas, line breaks, or quotes to be read correctly.                                                                                                                                                                                                                                                          |
| escape_char                | string  | no       | -                             | A single character that allows the quote or other special characters to appear inside a CSV field without ending the field.                                                                                                                                                                                                                                                     |

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
at the same time.

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

### quote_char [string]

A single character that encloses CSV fields, allowing fields with commas, line breaks, or quotes to be read correctly.

### escape_char [string]

A single character that allows the quote or other special characters to appear inside a CSV field without ending the field.

### schema [config]

#### fields [Config]

The schema of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

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
## Changelog

<ChangeLog />
