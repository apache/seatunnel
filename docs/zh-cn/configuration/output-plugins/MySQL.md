## Output plugin : Mysql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出数据到MySQL

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [password](#password-string) | string | yes | - |
| [save_mode](#save_mode-string) | string | no | append |
| [table](#table-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### password [string]

密码

##### save_mode [string]

存储模式，当前支持overwrite，append，ignore以及error。每个模式具体含义见[save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes)

##### table [string]

表名

##### url [string]

JDBC连接的URL。参考一个案例：`jdbc:mysql://localhose:3306/info`


##### user [string]

用户名

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)


### Example

```
mysql {
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    user = "username"
    password = "password"
    save_mode = "append"
}
```

> 将数据写入MySQL
