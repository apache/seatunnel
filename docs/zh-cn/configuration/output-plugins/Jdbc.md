## Output plugin : Jdbc

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

通过JDBC输出数据到外部数据源

### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- |--- |
| [driver](#driver-string) | string | yes | - |all streaming |
| [password](#password-string) | string | yes | - |all streaming |
| [save_mode](#save_mode-string) | string | no | append |spark streaming |
| [table](#table-string) | string | yes | - |all streaming |
| [url](#url-string) | string | yes | - |all streaming |
| [user](#user-string) | string | yes | - |all streaming |
| [jdbc.*](#jdbc.*-string) | string | no | - |all streaming |
| [jdbc_output_mode](#jdbc_output_mode-string) | string | no | replace |all streaming |
| [common-options](#common-options-string)| string | no | - | all streaming|


##### driver [string]

用来连接远端数据源的JDBC类名

##### password [string]

密码

##### save_mode [string]

存储模式，当前支持overwrite，append，ignore以及error。每个模式具体含义见[save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes)

##### table [string]

表名

##### url [string]

JDBC连接的URL。参考一个案例: `jdbc:postgresql://localhost/test`


##### user [string]

用户名

##### jdbc.* [string]

阿里durid连接池配置，详见https://github.com/alibaba/druid/wiki/DruidDataSource%E9%85%8D%E7%BD%AE%E5%B1%9E%E6%80%A7%E5%88%97%E8%A1%A8
在其列表属性之前添加jdbc.前缀，如配置initialSize(初始化连接池大小)，jdbc.initialSize="1"

##### jdbc_output_mode [string]

输出到jdbc的模式，支持两种模式`replace|insert ignore`,`insert ignore`如果主键重复会丢弃新数据不会报错，`replace`新数据会替代旧数据

用户名

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)


### Example
> spark streaming
```
jdbc {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    user = "username"
    password = "password"
    save_mode = "append"
}
```
> structured streaming
```
jdbc {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    user = "username"
    password = "password"
}
```

> 将数据通过JDBC写入MySQL
