# Jdbc

> Sink plugin : Jdbc [Spark]

## Description

Support `Update` to output data to Relational database

## Options

| name             | type   | required | default value |
|------------------| ------ |----------|---------------|
| driver           | string | yes      | -             |
| url              | string | yes      | -             |
| user             | string | yes      | -             |
| password         | string | yes      | -             |
| dbTable          | string | yes      | -             |
| saveMode         | string | no       | error         |
| useSsl           | string | no       | false         |
| customUpdateStmt | string | no       | -             |
| duplicateIncs    | string | no       | -             |
| showSql          | string | no       | true          |

### url [string]

The URL of the JDBC connection. Refer to a case: `jdbc:mysql://localhost/dbName`

### user [string]

username

##### password [string]

user password

### dbTable [string]

Source data table name

### saveMode [string]

Storage mode, add mode `update` , perform data overwrite in a specified way when inserting data key conflicts

Basic mode, currently supports `overwrite` , `append` , `ignore` and `error` . For the specific meaning of each mode, see [save-modes](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)

### useSsl [string]

Configure when `saveMode` is specified as `update` , whether to enable ssl, the default value is `false`

### customUpdateStmt [string]

Configure when `saveMode` is specified as `update` , which is used to specify the update statement template for key conflicts

Refer to the usage of `INSERT INTO table (...) values (...) ON DUPLICATE KEY UPDATE... ` of `mysql` , use placeholders or fixed values in `values`

### duplicateIncs [string]

Configure when `saveMode` is specified as `update` , and when the specified key conflicts, the value is updated to the existing value plus the original value

### showSql

Configure when `saveMode` is specified as `update` , whether to show sql

## Examples

```bash
jdbc {
    saveMode = "update",
    truncate = "true",
    url = "jdbc:mysql://ip:3306/database",
    user = "userName",
    password = "***********",
    dbTable = "tableName",
    customUpdateStmt = "INSERT INTO table (column1, column2, created, modified, yn) values(?, ?, now(), now(), 1) ON DUPLICATE KEY UPDATE column1 = IFNULL(VALUES (column1), column1), column2 = IFNULL(VALUES (column2), column2)"
}
```
