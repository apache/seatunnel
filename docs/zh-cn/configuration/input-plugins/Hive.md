## Input plugin : Hive

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从hive中获取数据

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
