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
