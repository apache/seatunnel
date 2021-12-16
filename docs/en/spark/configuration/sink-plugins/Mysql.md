# Sink plugin : Mysql [Spark]

## Description

Support `Update` to output data to `Mysql`

## Options

| name             | type   | required | default value |
| ---------------- | ------ | -------- | ------------- |
| url              | string | yes      | -             |
| user             | string | yes      | -             |
| password         | string | yes      | -             |
| save_mode        | string | yes      | -             |
| dbtable          | string | yes      | -             |
| customUpdateStmt | string | no       | -             |
| duplicateIncs    | string | no       | -             |

### url [string]

The URL of the JDBC connection. Refer to a case: `jdbc:mysql://localhost/dbName`

### user [string]

username

##### password [string]

user password

### save_mode [string]

Storage mode, add mode `update` , perform data overwrite in a specified way when inserting data key conflicts

Basic mode, currently supports `overwrite` , `append` , `ignore` and `error` . For the specific meaning of each mode, see [save-modes](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)

### dbtable [string]

Source data table name

### customUpdateStmt [string]

Configure when `save_mode` is specified as `update` , which is used to specify the update statement template for key conflicts

Refer to the usage of `INSERT INTO table (...) values (...) ON DUPLICATE KEY UPDATE... ` of `mysql` , use placeholders or fixed values in `values`

### duplicateIncs [string]

Configure when `save_mode` is specified as `update` , and when the specified key conflicts, the value is updated to the existing value plus the original value

## Examples

```bash
Mysql {
	save_mode = "update",
	truncate = true,
	url = "jdbc:mysql://ip:3306/database",
	user= "userName",
	password = "***********",
	dbtable = "tableName",
	customUpdateStmt = "INSERT INTO table (column1, column2, created, modified, yn) values(?, ?, now(), now(), 1) ON DUPLICATE KEY UPDATE column1 = IFNULL(VALUES (column1), column1), column2 = IFNULL(VALUES (column2), column2)"
}
```
