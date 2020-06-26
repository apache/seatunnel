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
| [jdbc.*](#jdbc.*-string) | string | no | - |structured streaming |
| [output_sql](#output_sql-string) | string | yes | - |structured streaming |
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

阿里druid连接池配置，详见https://github.com/alibaba/druid/wiki/DruidDataSource%E9%85%8D%E7%BD%AE%E5%B1%9E%E6%80%A7%E5%88%97%E8%A1%A8
在其列表属性之前添加jdbc.前缀，如配置initialSize(初始化连接池大小)，jdbc.initialSize="1"

##### output_sql [string]

输出到jdbc的sql，例如 `insert into test(age,name,city) values(?,?,?)`。注意的是，字段的顺序需要与`source_table_name(来自input或者filter)`的schema顺序一致

用户名

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/v1/configuration/output-plugin)


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
    output_sql = "insert into test(age,name,city) values(?,?,?)"
}
```

