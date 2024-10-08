# FtpFile

> Ftp file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
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

## Description

Read data from ftp file server.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

:::

## Options

|           name            |  type   | required |    default value    |
|---------------------------|---------|----------|---------------------|
| host                      | string  | yes      | -                   |
| port                      | int     | yes      | -                   |
| user                      | string  | yes      | -                   |
| password                  | string  | yes      | -                   |
| path                      | string  | yes      | -                   |
| file_format_type          | string  | yes      | -                   |
| connection_mode           | string  | no       | active_local        |
| delimiter/field_delimiter | string  | no       | \001                |
| read_columns              | list    | no       | -                   |
| parse_partition_from_path | boolean | no       | true                |
| date_format               | string  | no       | yyyy-MM-dd          |
| datetime_format           | string  | no       | yyyy-MM-dd HH:mm:ss |
| time_format               | string  | no       | HH:mm:ss            |
| skip_header_row_number    | long    | no       | 0                   |
| schema                    | config  | no       | -                   |
| sheet_name                | string  | no       | -                   |
| xml_row_tag               | string  | no       | -                   |
| xml_use_attr_format       | boolean | no       | -                   |
| file_filter_pattern       | string  | no       | -                   |
| compress_codec            | string  | no       | none                |
| archive_compress_codec    | string  | no       | none                |
| encoding                  | string  | no       | UTF-8               |
| common-options            |         | no       | -                   |

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

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

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

### connection_mode [string]

The target ftp connection mode , default is active mode, supported as the following modes:

`active_local` `passive_local`

### delimiter/field_delimiter [string]

**delimiter** parameter will deprecate after version 2.3.5, please use **field_delimiter** instead.

Only need to be configured when file_format is text.

Field delimiter, used to tell connector how to slice and dice fields.

default `\001`, the same as hive's default delimiter

### parse_partition_from_path [boolean]

Control whether parse the partition keys and values from file path

For example if you read a file from path `ftp://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

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
| NONE                   | all                | .*                      |

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to read. This param will be parsed by `Charset.forName(encoding)`.

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

LocalFile {
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
  FtpFile {
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    path = "/seatunnel/read/binary/"
    file_format_type = "binary"
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

## Changelog

### 2.2.0-beta 2022-09-26

- Add Ftp Source Connector

### 2.3.0-beta 2022-10-20

- [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/seatunnel/pull/2980))
- [Improve] Support extract partition from SeaTunnelRow fields ([3085](https://github.com/apache/seatunnel/pull/3085))
- [Improve] Support parse field from file path ([2985](https://github.com/apache/seatunnel/pull/2985))

