## Transform plugin : SQL [Spark]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description

使用SQL处理数据，支持Spark丰富的[UDF函数](http://spark.apache.org/docs/latest/api/sql/)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [sql](#sql-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |

##### sql [string]

SQL语句，SQL中使用的表名为 `Source` 或 `Transform` 插件中配置的 `result_table_name`

##### common options [string]

`Transform` 插件通用参数，详情参照 [Transform Plugin](/zh-cn/v2/spark/configuration/transform-plugins/)


### Examples

```
sql {
    sql = "select username, address from user_info",
}
```

> 使用SQL插件用于字段删减，仅保留 `username` 和 `address` 字段，将丢弃其余字段。`user_info` 为之前插件配置的 `result_table_name`

```
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
}
```

> 使用SQL插件用于数据处理，使用[substring functions](http://spark.apache.org/docs/latest/api/sql/#substring)对 `telephone` 字段进行截取操作

```
sql {
    sql = "select avg(age) from user_info",
    table_name = "user_info"
}
```

> 使用SQL插件用于数据聚合，使用[avg functions](http://spark.apache.org/docs/latest/api/sql/#avg)对原始数据集进行聚合操作，取出 `age` 字段平均值

