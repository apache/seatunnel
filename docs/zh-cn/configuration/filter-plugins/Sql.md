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

> 使用[substring functions](http://spark.apache.org/docs/latest/api/sql/#substring)对`telephone`字段进行截取操作

```
sql {
    sql = "select avg(age) from user_info",
    table = "user_info"
}
```

>  使用[avg functions](http://spark.apache.org/docs/latest/api/sql/#avg)对原始数据集进行聚合操作，取出`age`平均值
