# Sink plugin: Doris

### 描述

向Doris表中写数据

### 配置

| 配置项 | 类型 | 必填 | 默认值 | 支持引擎 |
| --- | --- | --- | --- | --- |
| fenodes | string | yes | - | Flink |
| db_name | string | yes | - | Flink |
| table_name | string | yes | - | Flink |
| username	 | string | yes | - | Flink |
| password	 | string | yes | - | Flink |
| doris_sink_batch_size	 | int | no |  5000 | Flink |
| doris_sink_interval	 | int | no |3000 | Flink |
| doris_sink_max_retries	 | int | no | 1 | Flink |
| doris_sink_properties.*	 | - | no | - | Flink |

##### fenodes [string]

Doris FE http 地址

##### db_name [string]

Doris 数据库名称

##### table_name [string]

Doris 表名称

##### username [string]

Doris 用户名

##### password [string]

Doris 密码

##### doris_sink_batch_size [int]

单次写Doris的最大行数,默认值100

##### doris_sink_interval [int]

flush 间隔时间(毫秒)，超过该时间后异步线程将 缓存中数据写入Doris。设置为0表示关闭定期写入。

##### doris_sink_max_retries [int]

写Doris失败之后的重试次数

##### doris_sink_properties.* [string]

Stream load 的导入参数。例如:'doris_sink_properties.column_separator' = ', ' 定义列分隔符

### Examples

```
DorisSink {
	 fenodes = "127.0.0.1:8030"
	 db_name = database
	 table_name = table
	 username = root
	 password = password
	 doris_sink_size = 1
}
```
