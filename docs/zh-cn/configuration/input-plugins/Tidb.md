## Input plugin : TiDB

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.5

### Description

通过[TiSpark](https://github.com/pingcap/tispark)从[TiDB](https://github.com/pingcap/tidb)数据库中读取数据，当前仅仅支持Spark 2.1

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [database](#database-string) | string | yes | - |
| [pre_sql](#pre_sql-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |

##### database [string]

TiDB库名

##### pre_sql [string]

进行预处理的sql, 如果不需要预处理,可以使用select * from tidb_db.tidb_table

##### table_name [string]

经过pre_sql获取到的数据，注册成临时表的表名



### Example


使用TiDB Input必须在`spark-defaults.conf`或者Waterdrop配置文件中配置`spark.tispark.pd.addresses`和`spark.sql.extensions`。

一个Waterdrop读取TiDB数据的配置文件如下：

```
spark {
  ...
  spark.tispark.pd.addresses = "localhost:2379"
  spark.sql.extensions = "org.apache.spark.sql.TiExtensions"
}

input {
    tidb {
        database = "test"
        pre_sql = "select * from test.my_table"
        table_name = "myTable"
    }
}

filter {
   ...
}

output {
    ...
}
```
