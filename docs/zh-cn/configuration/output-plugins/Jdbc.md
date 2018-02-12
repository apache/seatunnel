## Output plugin : Jdbc

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

通过JDBC输出数据到外部数据源

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

用来连接远端数据源的JDBC类名

##### password [string]

密码

##### save_mode [string]

存储模式，当前支持overwrite，append，ignore以及error。每个模式具体含义见[save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes)

##### table [string]

表名

##### url [string]

JDBC连接的URL。参考一个案例: jdbc:postgresql://localhost/test


##### user [string]

用户名


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

> 将数据通过JDBC写入MySQL