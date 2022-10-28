# FtpFile

> Ftp file source connector

## Description

Read data from ftp file server.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format
    - [x] text
    - [x] csv
    - [x] json

## Options

| name                       | type    | required | default value       |
|----------------------------|---------|----------|---------------------|
| host                       | string  | yes      | -                   |
| port                       | int     | yes      | -                   |
| user                       | string  | yes      | -                   |
| password                   | string  | yes      | -                   |
| path                       | string  | yes      | -                   |
| type                       | string  | yes      | -                   |
| delimiter                  | string  | no       | \001                |
| parse_partition_from_path  | boolean | no       | true                |
| date_format                | string  | no       | yyyy-MM-dd          |
| datetime_format            | string  | no       | yyyy-MM-dd HH:mm:ss |
| time_format                | string  | no       | HH:mm:ss            |
| schema                     | config  | no       | -                   |
| common-options             |         | no       | -                   |

### host [string]

The target ftp host is required

### port [int]

The target ftp port is required

### username [string]

The target ftp username is required

### password [string]

The target ftp password is required

### path [string]

The source file path.

### delimiter [string]

Field delimiter, used to tell connector how to slice and dice fields when reading text files

default `\001`, the same as hive's default delimiter

### parse_partition_from_path [boolean]

Control whether parse the partition keys and values from file path

For example if you read a file from path `ftp://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`

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

### schema [config]

The schema information of upstream data.

### type [string]

File type, supported as the following file types:

`text` `csv` `json`

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

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

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

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Example

```hocon

  FtpFile {
    path = "/tmp/seatunnel/sink/text"
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    type = "text"
    schema = {
      name = string
      age = int
    }
    delimiter = "#"
  }

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Ftp Source Connector

### 2.3.0-beta 2022-10-20

- [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
- [Improve] Support extract partition from SeaTunnelRow fields ([3085](https://github.com/apache/incubator-seatunnel/pull/3085))
- [Improve] Support parse field from file path ([2985](https://github.com/apache/incubator-seatunnel/pull/2985))
