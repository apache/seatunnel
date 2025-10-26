import ChangeLog from '../changelog/connector-file-ftp.md';

# FtpFile

> Ftp file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [multimodal](../../concept/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format type
  - [x] text
  - [x] csv
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown

## Description

Read data from ftp file server.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

:::

## Options

| name                        | type    | required | default value               |
|-----------------------------|---------|----------|-----------------------------|
| host                        | string  | yes      | -                           |
| port                        | int     | yes      | -                           |
| user                        | string  | yes      | -                           |
| password                    | string  | yes      | -                           |
| path                        | string  | yes      | -                           |
| file_format_type            | string  | yes      | -                           |
| connection_mode             | string  | no       | active_local                |
| remote_verification_enabled | boolean | no       | true                        |
| delimiter/field_delimiter   | string  | no       | \001 for text and , for csv |
| row_delimiter               | string  | no       | \n                          |
| read_columns                | list    | no       | -                           |
| parse_partition_from_path   | boolean | no       | true                        |
| date_format                 | string  | no       | yyyy-MM-dd                  |
| datetime_format             | string  | no       | yyyy-MM-dd HH:mm:ss         |
| time_format                 | string  | no       | HH:mm:ss                    |
| skip_header_row_number      | long    | no       | 0                           |
| schema                      | config  | no       | -                           |
| sheet_name                  | string  | no       | -                           |
| xml_row_tag                 | string  | no       | -                           |
| xml_use_attr_format         | boolean | no       | -                           |
| csv_use_header_line         | boolean | no       | -                           |
| file_filter_pattern         | string  | no       | -                           |
| filename_extension          | string  | no       | -                           |
| compress_codec              | string  | no       | none                        |
| archive_compress_codec      | string  | no       | none                        |
| encoding                    | string  | no       | UTF-8                       |
| null_format                 | string  | no       | -                           |
| binary_chunk_size           | int     | no       | 1024                        |
| binary_complete_file_mode   | boolean | no       | false                       |
| common-options              |         | no       | -                           |
| file_filter_modified_start  | string  | no       | -                           | 
| file_filter_modified_end    | string  | no       | -                           | 

### host [string]

The target ftp host is required

### port [int]

The target ftp port is required

### user [string]

The target ftp user name is required

### password [string]

The target ftp password is required

### path [string]

The source file path.

### remote_verification_enabled [boolean]

Whether to enable remote host verification for FTP data channels, default is `true`.

### file_filter_pattern [string]

Filter pattern, which used for filtering files.

The pattern follows standard regular expressions. For details, please refer to https://en.wikipedia.org/wiki/Regular_expression.
There are some examples.

File Structure Example:

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
/data/seatunnel/20241001/.*\.txt
```

The result of this example matching is:

```
/data/seatunnel/20241001/report.txt
```

**Example 2**: *Match all file starting with abc*，Regular Expression:

```
/data/seatunnel/20241002/abc.*
```

The result of this example matching is:

```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```

**Example 3**: *Match all file starting with abc，And the fourth character is either h or g*, the Regular Expression:

```
/data/seatunnel/20241007/abc[h,g].*
```

The result of this example matching is:

```
/data/seatunnel/20241007/abch202410.csv
```

**Example 4**: *Match third level folders starting with 202410 and files ending with .csv*, the Regular Expression:

```
/data/seatunnel/202410\d*/.*\.csv
```

The result of this example matching is:

```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### filename_extension [string]

Filter filename extension, which used for filtering files with specific extension. Example: `csv` `.txt` `json` `.xml`.

### file_format_type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

If you assign file type to `json` , you should also assign schema option to tell connector how to parse data to the row you want.

For example:

upstream data is the following:

```json

{"code":  200, "data":  "get success", "success":  true}

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

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

If you assign file type to `text` `csv`, you can choose to specify the schema information or not.

For example, upstream data is the following:

```text

tyrantlucifer#26#male

```

If you do not assign data schema connector will treat the upstream data as the following:

| content               |
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

| name          | age | gender |
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

### connection_mode [string]

The target ftp connection mode , default is active mode, supported as the following modes:

`active_local` `passive_local`

### control_encoding [string]

Character encoding for FTP control connection. Default is `UTF-8`.

When file paths contain special characters (such as `$`, spaces, Chinese characters, etc.),
this should be set to `UTF-8` to ensure paths can be parsed correctly.

For example: `/data/whale_ops/share/$Fund-Product/DA - SANY （三一）/Daily/2025.08.18/file.xlsx`

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

For example if you read a file from path `ftp://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

Every record data from file will be added these two fields:

| name          | age |
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

The schema information of upstream data.

### read_columns [list]

The read column list of the data source, user can use it to implement field projection.

### sheet_name [string]

Reader the sheet of the workbook,Only used when file_format_type is excel.

### xml_row_tag [string]

Only need to be configured when file_format is xml.

Specifies the tag name of the data rows within the XML file.

### xml_use_attr_format [boolean]

Only need to be configured when file_format is xml.

Specifies Whether to process data using the tag attribute format.

### csv_use_header_line [boolean]

Whether to use the header line to parse the file, only used when the file_format is `csv` and the file contains the header line that match RFC 4180

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

### file_filter_modified_start [string]

File modification time filter. The connector will filter some files base on the last modification start time (include start time). The default data format is `yyyy-MM-dd HH:mm:ss`.

### file_filter_modified_end [string]

File modification time filter. The connector will filter some files base on the last modification end time (not include end time). The default data format is `yyyy-MM-dd HH:mm:ss`.

### common options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.

## Example

```hocon

  FtpFile {
    path = "/tmp/seatunnel/sink/text"
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    file_format_type = "text"
    schema = {
      name = string
      age = int
    }
    field_delimiter = "#"
  }

```

### Multiple Table

```hocon

FtpFile {
  tables_configs = [
    {
      schema {
        table = "student"
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

```hocon

FtpFile {
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
  FtpFile {
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
    binary_chunk_size = 2048
    binary_complete_file_mode = false
  }
}
sink {
  // you can transfer local file to s3/hdfs/oss etc.
  FtpFile {
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
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
  FtpFile {
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
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

