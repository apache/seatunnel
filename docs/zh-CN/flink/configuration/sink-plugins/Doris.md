# Sink plugin: Doris [Flink]

### 描述

向Doris表中写数据

### 配置

| 配置项 | 类型 | 必填 | 默认值 | 支持引擎 |
| --- | --- | --- | --- | --- |
| fenodes | string | yes | - | Flink |
| database | string | yes | - | Flink  |
| table | string | yes | - | Flink  |
| user	 | string | yes | - | Flink  |
| password	 | string | yes | - | Flink  |
| batch_size	 | int | no |  100 | Flink  |
| interval	 | int | no |1000 | Flink |
| max_retries	 | int | no | 1 | Flink|
| doris.*	 | - | no | - | Flink  |

##### fenodes [string]

Doris FE http 地址

##### database [string]

Doris 数据库名称

##### table [string]

Doris 表名称

##### user [string]

Doris 用户名

##### password [string]

Doris 密码

##### batch_size [int]

单次写Doris的最大行数,默认值100

##### interval [int]

flush 间隔时间(毫秒)，超过该时间后异步线程将 缓存中数据写入Doris。设置为0表示关闭定期写入。

##### max_retries [int]

写Doris失败之后的重试次数

##### doris.* [string]

Stream load 的导入参数。例如:'doris.column_separator' = ', ' 定义列分隔符

### Examples

```
DorisSink {
	 fenodes = "127.0.0.1:8030"
	 database = database
	 table = table
	 user = root
	 password = password
	 batch_size = 1
	 doris.column_separator="\t"
     doris.columns="id,user_name,user_name_cn,create_time,last_login_time"
}
 ```
