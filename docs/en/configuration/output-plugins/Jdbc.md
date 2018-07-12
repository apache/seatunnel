## Output plugin : Jdbc

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Write Rows to an external data source via JDBC.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [driver](#driver-string) | string | yes | - |
| [password](#password-string) | string | yes | - |
| [save_mode](#save_mode-string) | string | no | append |
| [table](#table-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |

##### driver [string]

Class name of driver.

##### password [string]

Password.

##### save_mode [string]

Save mode, supports `overwrite`, `append`, `ignore` and `error`. The detail of save_mode see [save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes).

##### table [string]

Table name.

##### url [string]

The url of JDBC. For example: `jdbc:postgresql://localhost/test`


##### user [string]

Username.


### Example

```
jdbc {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhose:3306/info"
    table = "access"
    user = "username"
    password = "password"
    save_mode = "append"
}
```