## Input plugin : Mysql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

读取MySQL的数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [password](#password-string) | string | yes | - |
| [table](#table-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |


##### password [string]

密码


##### table [string]

表名


##### table_name [string]

注册为Spark临时表的表名


##### url [string]

JDBC连接的URL。参考一个案例：`jdbc:mysql://localhost:3306/info`


##### user [string]

用户名


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

> 从MySQL中读取数据
