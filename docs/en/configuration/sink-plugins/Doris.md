# Sink plugin: Doris

### Description

Write Rows to a Doris Table.

### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| fenodes | string | yes | - | Flink |
| db_name | string | yes | - | Flink |
| table_name | string | yes | - | Flink |
| username	 | string | yes | - | Flink |
| password	 | string | yes | - | Flink |
| doris_sink_batch_size	 | int | no |  100 | Flink |
| doris_sink_interval	 | int | no |1000 | Flink |
| doris_sink_max_retries	 | int | no | 1 | Flink |
| doris_sink_properties.*	 | - | no | - | Flink |

##### fenodes [string]

Doris FE http address

##### db_name [string]

Doris database name

##### table_name [string]

Doris table name

##### username [string]

Doris username

##### password [string]

Doris password

##### doris_sink_batch_size [int]

Maximum number of lines in a single write Doris,default value is 100.

##### doris_sink_interval [int]

The flush interval millisecond, after which the asynchronous thread will write the data in the cache to Doris.Set to 0 to turn off periodic writing.

##### doris_sink_max_retries [int]

Number of retries after writing Doris failed

##### doris_sink_properties.* [string]

The doris stream load parameters.eg:doris_sink_properties.column_separator' = ','

=======
# Sink plugin: Doirs

### Description:
Use Spark Batch Engine ETL Data to Doris.

### Options
| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| host | string | yes | - | Spark |
| database | string | yes | - | Spark |
| tableName	 | string | yes | - | Spark |
| user	 | string | yes | - | Spark |
| password	 | string | yes | - | Spark |
| bulk_size	 | int | yes | - | Spark |
| doris.*	 | string | no | - | Spark |

##### host [string]
Doris FE address:8030

##### database [string]
Doris target database name
##### tableName [string]
Doris target table name
##### user [string]
Doris user name
##### password [string]
Doris user's password
##### bulk_size [string]
Doris number of submissions per batch
##### doris. [string]
Doris stream_load properties,you can use 'doris.' prefix + stream_load properties

[More Doris stream_load Configurations](https://doris.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html)

### Examples

```

DorisSparkSink {
	 fenodes = "127.0.0.1:8030"
	 db_name = database
	 table_name = table
	 username = root
	 password = password
	 doris_sink_size = 1
}

DorisFlinkSink {
      host="0.0.0.0:8030"
      database="test"
      tableName="user"
      user="doris"
      password="doris"
      bulk_size=10000
      doris.column_separator="\t"
      doris.columns="id,user_name,user_name_cn,create_time,last_login_time"
}
 ```
