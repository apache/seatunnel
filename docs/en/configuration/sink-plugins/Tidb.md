# Sink plugin: Tidb

### Description

Write data to Tidb.

### Options

| name             | type   | required | default value |
|------------------| ------ |----------|---------------|
| [url](#url-string)              | string | yes      | -             |
| [user](#user-string)             | string | yes      | -             |
| [password](#password-string)         | string | yes      | -             |
| [table](#table-string)            | string | yes      | -             |
| [save_mode](#save_mode-string)        | string | no       | append        |
| [useSSL](#useSSL-string)           | string | no       | false         |
| [isolationLevel](#isolationLevel-string)    | string | no       | NONE           |
| [batchSize](#batchSize-int)    | int | no       | 150           |

##### url [string]

The url of the tidb jdbc connection. Refer to a case: `jdbc:mysql://ip:port/dbName`

##### user [string]

Username

##### password [string]

User Password

#### table [string]

Source Data Table Name

##### save_mode [string]

Storage mode, currently supports `overwrite` , `append` , `ignore` and `error` . For the specific meaning of each mode, see [save-modes](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)

##### useSSL [string]

The default value is `false`

##### isolationLevel [string]

Recommended to set isolationLevel to NONE if you have a large DF to load.

##### batchSize [int]

Jdbc Batch Insert Size

### Examples

```bash
tidb {
    url = "jdbc:mysql://ip:3306/database",
    user = "userName",
    password = "***********",
    table = "tableName"
}
```
