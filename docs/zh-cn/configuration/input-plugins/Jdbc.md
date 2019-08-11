## Input plugin : Jdbc

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

通过JDBC读取外部数据源数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [driver](#driver-string) | string | yes | - |
| [password](#password-string) | string | yes | - |
| [table](#table-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |
| [common-options](#common-options-string)| string | yes | - |


##### driver [string]

用来连接远端数据源的JDBC类名

##### password [string]

密码

##### table [string]

表名


##### url [string]

JDBC连接的URL。参考一个案例: `jdbc:postgresql://localhost/test`


##### user [string]

用户名

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


### Example

```
jdbc {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    result_table_name = "access_log"
    user = "username"
    password = "password"
}
```

> 通过JDBC读取MySQL数据
