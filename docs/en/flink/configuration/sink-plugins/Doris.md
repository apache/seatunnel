# Doris

> Sink plugin: Doris [Flink]

### Description

Write Data to a Doris Table.

### Options

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
| parallelism | int | no  | - |Flink|

##### fenodes [string]

Doris FE http address

##### database [string]

Doris database name

##### table [string]

Doris table name

##### user [string]

Doris username

##### password [string]

Doris password

##### batch_size [int]

Maximum number of lines in a single write Doris,default value is 100.

##### interval [int]

The flush interval millisecond, after which the asynchronous thread will write the data in the cache to Doris.Set to 0 to turn off periodic writing.

##### max_retries [int]

Number of retries after writing Doris failed

##### doris.* [string]

The doris stream load parameters.you can use 'doris.' prefix + stream_load properties. eg:doris.column_separator' = ','
[More Doris stream_load Configurations](https://doris.apache.org/administrator-guide/load-data/stream-load-manual.html)

### parallelism [Int]

The parallelism of an individual operator, for DorisSink

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
