## Output plugin : Mysql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Write Rows to MySQL.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [password](#password-string) | string | yes | - |
| [save_mode](#save_mode-string) | string | no | append |
| [table](#table-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |


##### password [string]

Password.

##### save_mode [string]

Save mode, supports `overwrite`, `append`, `ignore` and `error`. The detail of save_mode see [save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes).

##### table [string]

Table name.

##### url [string]

The url of JDBC. For example: `jdbc:mysql://localhose:3306/info`


##### user [string]

Username.


### Example

```
mysql {
    url = "jdbc:mysql://localhose:3306/info"
    table = "access"
    user = "username"
    password = "password"
    save_mode = "append"
}
```