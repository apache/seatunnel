# HdfsFile

> Hdfs file source connector

## Description

Read data from hdfs file system.

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

| name          | type   | required | default value |
|---------------|--------|----------|---------------|
| path          | string | yes      | -             |
| type          | string | yes      | -             |
| fs.defaultFS  | string | yes      | -             |
| schema        | config | no       | -             |

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

If you assign file type to `text` `csv`, schema option not supported temporarily, but the subsequent features will support.

Now connector will treat the upstream data as the following:

| lines                             |
|-----------------------------------|
| The content of every line in file |

### fs.defaultFS [string]

Hdfs cluster address.

## Example

```hocon

HdfsFile {
  path = "/apps/hive/demo/student"
  type = "parquet"
  fs.defaultFS = "hdfs://namenode001"
}

```

```hocon

HdfsFile {
  schema {
    fields {
      name = string
      age = int
    }
  }
  path = "/apps/hive/demo/student"
  type = "json"
  fs.defaultFS = "hdfs://namenode001"
}

```