## Input plugin : Mysql

* Author: InterestingLab
* Homepage: <https://interestinglab.github.io/waterdrop>
* Version: 1.0.0

### Description

读取MySQL的数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [password](#password-string) | string | yes | - |
| [jdbc.*](#jdbc-string) | string| no ||
| [table](#table-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |
| [common-options](#common-options-string)| string | yes | - |


##### password [string]

密码


##### jdbc [string]

除了以上必须指定的参数外，用户还可以指定多个非必须参数，覆盖了Spark JDBC提供的所有[参数](https://spark.apache.org/docs/2.4.0/sql-programming-guide.html#jdbc-to-other-databases).

指定参数的方式是在原参数名称上加上前缀"jdbc."，如指定fetchsize的方式是: jdbc.fetchsize = 50000。如果不指定这些非必须参数，它们将使用Spark JDBC给出的默认值。


##### table [string]

表名


##### url [string]

JDBC连接的URL。参考一个案例：`jdbc:mysql://localhost:3306/info`


##### user [string]

用户名

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


### Example

```
mysql {
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    result_table_name = "access_log"
    user = "username"
    password = "password"
}
```

> 从MySQL中读取数据

```
mysql {
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    result_table_name = "access_log"
    user = "username"
    password = "password"
    jdbc.partitionColumn = "item_id"
    jdbc.numPartitions = "10"
    jdbc.lowerBound = 0
    jdbc.upperBound = 100
}
```

> 根据指定字段划分分区