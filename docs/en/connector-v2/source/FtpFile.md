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

| name     | type   | required | default value |
|----------|--------|----------|---------------|
| host     | string | yes      | -             |
| port     | int    | yes      | -             |
| user     | string | yes      | -             |
| password | string | yes      | -             |
| path     | string | yes      | -             |
| type     | string | yes      | -             |
| schema   | config | no       | -             |

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

If you assign file type to `text` `csv`, schema option not supported temporarily, but the subsequent features will support.

Now connector will treat the upstream data as the following:

| lines                             |
|-----------------------------------|
| The content of every line in file |

### schema [config]

The schema information of upstream data.

## Example

```hocon

  FtpFile {
    path = "/tmp/seatunnel/sink/parquet"
    host = "192.168.31.48"
    port = 21
    user = tyrantlucifer
    password = tianchao
    type = "text"
  }

```
