# Sink plugin: Doris [Flink]

### 描述

将数据写入 Doris

### 配置

| name | type | required | default value | engine |
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

Doris 表名

##### user [string]

Doris 用户名

##### password [string]

Doris 密码

##### batch_size [int]

单次写入 Doris 的最大行数,默认值为：100

##### interval [int]

flush 间隔时间(毫秒)，超过时间后异步线程将缓存中数据写入 Doris。设置为 0 表示关闭定期写入。

##### max_retries [int]

写 Doris 失败后的重试次数

##### doris.* [string]

Stream load 的导入参数。例如:'doris.column_separator' = ', ' 定义列分隔符 [More Doris stream_load Configurations](https://doris.apache.org/administrator-guide/load-data/stream-load-manual.html)

#### parallelism [Int]

DorisSink 单个算子的并行性

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
