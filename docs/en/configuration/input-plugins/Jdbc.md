## Input plugin : Jdbc

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Read data from an external data source via JDBC.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [driver](#driver-string) | string | yes | - |
| [password](#password-string) | string | yes | - |
| [table](#table-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |

##### driver [string]

Class name of jdbc driver.

##### password [string]

Password.


##### table [string]

Table name.


##### table_name [string]

Registered table name of input data.


##### url [string]

The url of JDBC. For example: `jdbc:postgresql://localhost/test`


##### user [string]

Username.


### Example

```
jdbc {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    table_name = "access_log"
    user = "username"
    password = "password"
}
```

> Read data from MySQL with jdbc.
