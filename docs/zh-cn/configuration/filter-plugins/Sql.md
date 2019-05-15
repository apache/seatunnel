## Filter plugin : Sql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

使用SQL处理数据，支持Spark丰富的[UDF函数](http://spark.apache.org/docs/latest/api/sql/)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [sql](#sql-string) | string | yes | - |
| [table_name](#table_name-string) | string | no | - |

##### sql [string]

SQL语句

##### table_name [string]

如果配置了`table_name`, 代表将输入sql filter的数据流注册为一个运行时的临时表，它的表名由`table_name`指定。
这个表可以在`sql`中使用。

此参数主要用于将流式的数据，注册为表，之后再通过`sql`做计算。所以当`sql`中需要处理流式数据时，必须要配置此参数。
否则会报错`Table or view not found`。

一般情况下，当input中配置了静态的数据源(Static Input)时，这些静态数据源已经在input中注册了对应的`table_name`，
再这里不需要注册了，可直接使用。

### Examples

```
sql {
    sql = "select username, address from user_info",
    table_name = "user_info"
}
```

> 仅保留`username`和`address`字段，将丢弃其余字段

```
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
    table_name = "user_info"
}
```

> 使用[substring functions](http://spark.apache.org/docs/latest/api/sql/#substring)对`telephone`字段进行截取操作

```
sql {
    sql = "select avg(age) from user_info",
    table_name = "user_info"
}
```

>  使用[avg functions](http://spark.apache.org/docs/latest/api/sql/#avg)对原始数据集进行聚合操作，取出`age`平均值
