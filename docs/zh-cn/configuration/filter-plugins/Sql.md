## Filter plugin : Sql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

使用SQL处理数据，支持Spark丰富的[UDF](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [sql](#sql-string) | string | yes | - |
| [table](#table-string) | string | yes | - |

##### sql [string]

SQL语句

##### table [string]

表名，可为任意字符串, 这也是sql参数中使用的表名

### Examples

```
sql {
    sql = "select username, address from user_info",
    table = "user_info"
}
```

> 仅保留`username`和`address`字段，将丢弃其余字段

```
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
    table = "user_info"
}
```

> 使用Spark提供的[String functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$)对`telephone`字段进行截取操作

```
sql {
    sql = "select avg(age) from user_info",
    table = "user_info"
}
```

>  使用Spark提供的[Aggregate functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$)对原始数据集进行聚合操作，取出`age`平均值
