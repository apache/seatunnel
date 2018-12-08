## Input plugin : Mysql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.2

### Description

Read data from MySQL.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [password](#password-string) | string | yes | - |
| [table](#table-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |


##### password [string]

Password.

##### table [string]

Table name.


##### table_name [string]

Registered table name of input data.


##### url [string]

The url of JDBC. For example: `jdbc:mysql://localhost:3306/info`


##### user [string]

Username.


### Example

```
mysql {
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    table_name = "access_log"
    user = "username"
    password = "password"
}
```

> Read data from MySQL.