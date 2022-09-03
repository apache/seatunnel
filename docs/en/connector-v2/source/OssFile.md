# OssFile

> Oss file source connector

## Description

Read data from aliyun oss file system.

> Tips: We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to OSS and this connector need some hadoop dependencies. 
> It's only support hadoop version **2.9.X+**.

## Key features

- [x] batch
- [ ] stream
- [ ] exactly-once
- [x] schema projection
- [x] file format
    - [x] text
    - [x] csv
    - [x] parquet
    - [x] orc
    - [x] json

supports query SQL and can achieve projection effect.

- [x] parallelism

## Options

| name         | type   | required | default value |
|--------------|--------|----------|---------------|
| path         | string | yes      | -             |
| type         | string | yes      | -             |
| bucket       | string | yes      | -             |
| accessKey    | string | yes      | -             |
| accessSecret | string | yes      | -             |
| endpoint     | string | yes      | -             |
| schema       | config | no       | -             |

### path [string]

The source file path.

### type [string]

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

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

If you assign file type to `parquet` `orc`, schema option not required, connector can find the schema of upstream data automatically.

If you assign file type to `text` `csv`, schema option not supported temporarily, but the subsequent features will support.

Now connector will treat the upstream data as the following:

| lines                             |
|-----------------------------------|
| The content of every line in file |

### bucket [string]

The bucket address of oss file system, for example: `oss://tyrantlucifer-image-bed`

### accessKey [string]

The access key of oss file system.

### accessSecret [string]

The access secret of oss file system.

### endpoint [string]

The endpoint of oss file system.

### schema [config]

The schema of upstream data.

## Example

```hocon

  OssFile {
    path = "/seatunnel/orc"
    bucket = "oss://tyrantlucifer-image-bed"
    accessKey = "xxxxxxxxxxxxxxxxx"
    accessSecret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    type = "orc"
  }

```

```hocon

  OssFile {
    path = "/seatunnel/json"
    bucket = "oss://tyrantlucifer-image-bed"
    accessKey = "xxxxxxxxxxxxxxxxx"
    accessSecret = "xxxxxxxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    type = "json"
    schema {
      fields {
        id = int 
        name = string
      }
    }
  }

```