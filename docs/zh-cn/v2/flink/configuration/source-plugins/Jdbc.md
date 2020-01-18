## Source plugin : JDBC [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
通过jdbc的方式读取数据

### Options
| name | type | required | default value |
| --- | --- | --- | --- |
| [driver](#driver-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [username](#username-string) | string | yes | - |
| [password](#password-string) | string | no | - |
| [query](#query-string) | string | yes | - |
| [fetch_size](#fetch_size-int) | int | no | - |
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
查询语句

##### fetch_size [int]
拉取数量

##### common options [string]

`Source` 插件通用参数，详情参照 [Source Plugin](README.md)

### Examples

```
JdbcSource {
        driver = com.mysql.jdbc.Driver
        url = "jdbc:mysql://localhost/test"
        username = root
        query = "select * from test"
   }

```
