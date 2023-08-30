# SftpFile

> Sftp file source connector

## Description

Read data from sftp file server.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

:::

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

## Options

|           name            |  type   | required |    default value    |
|---------------------------|---------|----------|---------------------|
| host                      | string  | yes      | -                   |
| port                      | int     | yes      | -                   |
| user                      | string  | yes      | -                   |
| password                  | string  | yes      | -                   |
| path                      | string  | yes      | -                   |
| file_format_type          | string  | yes      | -                   |
| delimiter                 | string  | no       | \001                |
| parse_partition_from_path | boolean | no       | true                |
| date_format               | string  | no       | yyyy-MM-dd          |
| skip_header_row_number    | long    | no       | 0                   |
| datetime_format           | string  | no       | yyyy-MM-dd HH:mm:ss |
| time_format               | string  | no       | HH:mm:ss            |
| schema                    | config  | no       | -                   |
| common-options            |         | no       | -                   |
| sheet_name                | string  | no       | -                   |
| file_filter_pattern       | string  | no       | -                   |

### host [string]

The target sftp host is required

### port [int]

The target sftp port is required

### username [string]

The target sftp username is required

### password [string]

The target sftp password is required

### path [string]

The source file path.

### delimiter [string]

Field delimiter, used to tell connector how to slice and dice fields when reading text files

default `\001`, the same as hive's default delimiter

### parse_partition_from_path [boolean]

Control whether parse the partition keys and values from file path

For example if you read a file from path `sftp://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

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

The schema information of upstream data.

### read_columns [list]

The read column list of the data source, user can use it to implement field projection.

The file type supported column projection as the following shown:

- text
- json
- csv
- orc
- parquet
- excel

**Tips: If the user wants to use this feature when reading `text` `json` `csv` files, the schema option must be configured**

### file_format_type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json`

If you assign file type to `json`, you should also assign schema option to tell connector how to parse data to the row you want.

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

If you assign data schema, you should also assign the option `delimiter` too except CSV file type

you should assign schema and delimiter as the following:

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

connector will generate data as the following:

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

### sheet_name [string]

Reader the sheet of the workbook,Only used when file_format_type is excel.

### file_filter_pattern [string]

Filter pattern, which used for filtering files.

## Example

```hocon

  SftpFile {
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
    delimiter = "#"
  }

```

## Changelog

### next version

- Add SftpFile Source Connector

