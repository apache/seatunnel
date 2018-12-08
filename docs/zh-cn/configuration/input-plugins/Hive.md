## Input plugin : Hive

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.2

### Description

从hive中获取数据，

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [pre_sql](#pre_sql-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |


##### pre_sql [string]

进行预处理的sql, 如果不需要预处理,可以使用select * from hive_db.hive_table

##### table_name [string]

经过pre_sql获取到的数据，注册成临时表的表名



### Example

```
hive {
    pre_sql = "select * from mydb.mytb"
    table_name = "myTable"
}
```

### Notes
必须保证hive的metastore是在服务状态。启动命令 `hive --service metastore` 服务的默认端口的`9083`
cluster、client、local模式下必须把hive-site.xml置于提交任务节点的$HADOOP_CONF目录下,IDE本地调试将其放在resources目录

