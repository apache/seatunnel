# Sink plugin: Tidb

## Description

Write data to Tidb.

## Options

| name             | type   | required | default value |
|------------------| ------ |----------|---------------|
| url(#url-string)              | string | yes      | -             |
| user             | string | yes      | -             |
| password         | string | yes      | -             |
| table            | string | yes      | -             |
| save_mode        | string | no       | append        |
| useSSL           | string | no       | false         |
| isolationLevel   | string | no       | NONE          |
| batchsize        | string | no       | 150           |

### url [string]

The url of the tidb jdbc connection. Refer to a case: `jdbc:mysql://ip:port/dbName`

### user [string]

Username

#### password [string]

User Password

### table [string]

Source Data Table Name

### save_mode [string]

Storage mode, currently supports `overwrite` , `append` , `ignore` and `error` . For the specific meaning of each mode, see [save-modes](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)

### useSSL [string]

The default value is `false`

### isolationLevel [string]

Recommended to set isolationLevel to NONE if you have a large DF to load.

### batchsize [string]

Jdbc Batch Insert Size

## Examples

```bash
tidb {
    url = "jdbc:mysql://ip:3306/database",
    user = "userName",
    password = "***********",
    table = "tableName"
}
```
