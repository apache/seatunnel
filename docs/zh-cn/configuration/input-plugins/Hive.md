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
| [common-options](#common-options-string)| string | yes | - |


##### pre_sql [string]

进行预处理的sql, 如果不需要预处理,可以使用select * from hive_db.hive_table

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


**注意：从waterdrop v1.3.4 开始，使用hive input必须做如下配置：**

```
# Waterdrop 配置文件中的spark section中：

spark {
  ...
  spark.sql.catalogImplementation = "hive"
  ...
}

```


### Example

```
spark {
  ...
  spark.sql.catalogImplementation = "hive"
  ...
}

input {
  hive {
    pre_sql = "select * from mydb.mytb"
    result_table_name = "myTable"
  }
}

...
```

### Notes
必须保证hive的metastore是在服务状态。启动命令 `hive --service metastore` 服务的默认端口的`9083`
cluster、client、local模式下必须把hive-site.xml置于提交任务节点的$HADOOP_CONF目录下(或者放在$SPARK_HOME/conf下面),IDE本地调试将其放在resources目录

