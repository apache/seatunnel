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
| [common-options](#common-options-string)| string | no | - |


##### sql [string]

SQL语句，SQL中使用的表名为 `Input` 或 `Filter` 插件中配置的 `result_table_name`

##### table_name [string]

**\[从v1.4开始废弃\]**，后续 Release 版本中将删除此参数

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
sql {
    sql = "select username, address from user_info",
}
```

> 仅保留`username`和`address`字段，将丢弃其余字段。`user_info` 为之前插件配置的 `result_table_name`

```
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
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
