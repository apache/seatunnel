# S3File

> S3 file source connector

## Description

Read data from aws s3 file system.

> Tips: We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to S3 and this connector need some hadoop dependencies.
> It's only support hadoop version **2.6.5+**.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)

Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## Options

| name                      | type    | required | default value       |
|---------------------------|---------|----------|---------------------|
| path                      | string  | yes      | -                   |
| type                      | string  | yes      | -                   |
| bucket                    | string  | yes      | -                   |
| access_key                | string  | yes      | -                   |
| access_secret             | string  | yes      | -                   |
| delimiter                 | string  | no       | \001                |
| parse_partition_from_path | boolean | no       | true                |
| date_format               | string  | no       | yyyy-MM-dd          |
| datetime_format           | string  | no       | yyyy-MM-dd HH:mm:ss |
| time_format               | string  | no       | HH:mm:ss            |
| schema                    | config  | no       | -                   |
| common-options            |         | no       | -                   |

### path [string]

The source file path.

### delimiter [string]

Field delimiter, used to tell connector how to slice and dice fields when reading text files

default `\001`, the same as hive's default delimiter

### parse_partition_from_path [boolean]

Control whether parse the partition keys and values from file path

For example if you read a file from path `s3n://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

Every record data from file will be added these two fields:

| name           | age |
|----------------|-----|
| tyrantlucifer  | 26  |

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

### type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json`

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

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.

If you assign file type to `text` `csv`, you can choose to specify the schema information or not.

For example, upstream data is the following:

```text

tyrantlucifer#26#male

```

If you do not assign data schema connector will treat the upstream data as the following:

| content                |
|------------------------|
| tyrantlucifer#26#male  | 

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

| name          | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

### bucket [string]

The bucket address of s3 file system, for example: `s3n://seatunnel-test`

**Tips: SeaTunnel S3 file connector only support `s3n` protocol, not support `s3` and `s3a`**

### access_key [string]

The access key of s3 file system.

### access_secret [string]

The access secret of s3 file system.

### schema [config]

#### fields [Config]

The schema of upstream data.

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Example

```hocon

  S3File {
    path = "/seatunnel/text"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3n://seatunnel-test"
    type = "text"
  }

```

```hocon

  S3File {
    path = "/seatunnel/json"
    bucket = "s3n://seatunnel-test"
    access_key = "xxxxxxxxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxxxxxxx"
    type = "json"
    schema {
      fields {
        id = int 
        name = string
      }
    }
  }

```

## Changelog

### 2.3.0-beta 2022-10-20

- Add S3File Source Connector
