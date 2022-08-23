# Phoenix

> Phoenix sink connector

## Description
Write data to Phoenix. Support Batch mode and Streaming mode.
The tested Phoenix version is 4.14.0-cdh5.13.1.
On the underlying implementation, through the jdbc driver of Phoenix, execute the upsert statement to write data to HBase.
> Warn: for solve Jar package conflict , you have to provide Phoenix JDBC driver yourself.

>  e.g. if you use Phoenix (Thick) Driver, copy phoenix-core-xxx-HBase-xxx.jar to $SEATNUNNEL_HOME/lib for Standalone.

>  e.g. if you use Phoenix (Thin) Driver, copy phoenix-xxx-HBase-xxx-thin-client.jar to $SEATNUNNEL_HOME/lib for Standalone.



## Options

| name                         | type | required | default value |
| --- | --- | --- | --- |
| connect_url                  | String | Yes | - |
| user                         | String | No | - |
| password                     | String | No | - |
| connection_check_timeout_sec | Int | No | 30 |
| sink_table                   | String | Yes | - |
| sink_column                  | array | No | When this parameter is empty, all fields are sink columns |
| batch_size                   | Int | No | 256 |
| batch_interval_ms            | Int | No | 1000 |
| max_retries                  | Int | No | 3 |
| retry_backoff_multiplier_ms  | Int | No | 500 |
| max_retry_backoff_ms         | Int | No | 3000 |

### connect_url [string]
Two ways of connecting Phoenix with Java JDBC. One is to connect to zookeeper through JDBC, and the other is to connect to queryserver through JDBC thin client.
> USE Phoenix (Thick) Driver JDBC URL Refer to a Case: `jdbc:phoenix:localhost:2182/hbase`

> USE Phoenix (Thin) Driver JDBC URL Refer to a Case: `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`


### user [string]
userName

### password [string]
password

### sink_table [string]
The name of the table to be imported is case sensitive. Usually, the name of the phoenix table is **uppercase**

### sink_column [array]
Column name, case sensitive, usually phoenix column name is **uppercase**.
When this parameter is empty, all fields are sink columns

### batch_size[int]
For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the database

### batch_interval_ms[int]
For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the database

### max_retries[int]
The number of retries to submit failed (executeBatch)

### retry_backoff_multiplier_ms [int]

Using as a multiplier for generating the next delay for backoff

### max_retry_backoff_ms [int]

The amount of time to wait before attempting to retry a request to `phoenix`

### connection_check_timeout_sec [int]

The time in seconds to wait for the database operation used to validate the connection to complete.

## Example
use Thick client drive
```
Phoenix {

    connect_url = "jdbc:phoenix:localhost:2182/hbase"
    sink_table = "test"
    sink_columns=["name","age"]

    max_retries = 0
    batch_size = 0
}

```

use thin client drive
```
Phoenix {

    connect_url = "jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF"
    user = "root"
    password = "123456"
    sink_table = "test"
    sink_columns=["name","age"]

    max_retries = 0
    batch_size = 0
}
```

