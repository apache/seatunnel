## Sink plugin : JDBC [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
通过jdbc方式写入数据

### Options
| name | type | required | default value |
| --- | --- | --- | --- |
| [driver](#driver-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [username](#username-string) | string | yes | - |
| [password](#password-string) | string | no | - |
| [query](#query-string) | string | yes | - |
| [batch_size](#batch_size-int) | int | no | - |
| [source_table_name](#source_table_name-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |

##### driver [string]

驱动名，如`com.mysql.jdbc.Driver`

##### url [string]

JDBC连接的URL。如：`jdbc:mysql://localhost:3306/test`

##### username [string]

用户名

##### password [string]

密码

##### query [string]
插入语句

##### batch_size [int]
每批写入数量

##### common options [string]

`Sink` 插件通用参数，详情参照 [Sink Plugin](README.md)

### Examples
```
   JdbcSink {
     source_table_name = fake
     driver = com.mysql.jdbc.Driver
     url = "jdbc:mysql://localhost/test"
     username = root
     query = "insert into test(name,age) values(?,?)"
     batch_size = 2
   }
```
