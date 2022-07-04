# Apache Pulsar

## Description

Source connector for Apache Pulsar. It can support both off-line and real-time jobs.

##  Options

| name | type | required | default | description |
| --- | --- | --- | --- | --- |
| topic | String | No | - | Topic name(s) to read data from when the table is used as source.  It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'.   <br/> **Note,only one of "topic-pattern" and "topic" can be specified for sources.** |
| topic-pattern | String | No | - | The regular expression for a pattern of topic names to read from. All topics with names that match the specified regular expression will be subscribed by the consumer when the job starts running. <br/>**Note, only one of "topic-pattern" and "topic" can be specified for sources.** |
| topic-discovery.interval | Long | No | 30000 | The interval (in ms) for the Pulsar source to discover the new topic partitions. A non-positive value disables the topic partition discovery.<br/>**Note, This option only works if the 'topic-pattern' option is used.** |
| subscription.name | String | Yes | - | Specify the subscription name for this consumer. This argument is required when constructing the consumer. |
| client.service-url | String | Yes | - | Service URL provider for Pulsar service.<br/>To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.<br/>You can assign Pulsar protocol URLs to specific clusters and use the Pulsar scheme.<br/>This is an example of `localhost`: `pulsar://localhost:6650,localhost:6651`. |
| admin.service-url | String | Yes | - | The Pulsar service HTTP URL for the admin endpoint. For example, `http://my-broker.example.com:8080`, or `https://my-broker.example.com:8443` for TLS. |
| auth.plugin-class | String | No | - | Name of the authentication plugin. |
| auth.params | String | No | - | Parameters for the authentication plugin.<br/>Example: `key1:val1,key2:val2` |
| poll.timeout | Integer | No | 100 | The maximum time (in ms) to wait when fetching records. A longer time increases throughput but also latency. |
| poll.interval | Long | No | 50 | The interval time(in ms) when fetcing records.  A shorter time increases throughput, but also increases CPU load. |
| poll.batch.size | Integer | No | 500 | The maximum number of records to fetch to wait when polling. A longer time increases throughput but also latency. |
| cursor.startup.mode | Enum | No | LATEST | Startup mode for Pulsar consumer, valid values are `'EARLIEST'`, `'LATEST'`, `'SUBSCRIPTION'`, `'TIMESTAMP'`. |
| cursor.startup.timestamp | Long | No | - | Start from the specified epoch timestamp (in milliseconds).<br/>**Note, This option is required when the "cursor.startup.mode" option used `'TIMESTAMP'`.** |
| cursor.reset.mode | Enum | No | LATEST | Cursor reset strategy for Pulsar consumer valid values are `'EARLIEST'`, `'LATEST'`.<br/>**Note, This option only works if the "cursor.startup.mode" option used `'SUBSCRIPTION'`.** |
| cursor.stop.mode | Enum | No | NEVER | Stop mode for Pulsar consumer, valid values are `'NEVER'`, `'LATEST'`and `'TIMESTAMP'`. <br/>**Note, When `'NEVER' `is specified, it is a real-time job, and other mode are off-line jobs.** |
| cursor.stop.timestamp | Long | No | - | Stop from the specified epoch timestamp (in milliseconds).<br/>**Note, This option is required when the "cursor.stop.mode" option used `'TIMESTAMP'`.** |

## Example

```Jdbc {
source {
  
  Pulsar {
  	topic = "example"
  	subscription.name = "seatunnel"
    client.service-url = "localhost:pulsar://localhost:6650"
    admin.service-url = "http://my-broker.example.com:8080"
    result_table_name = "test"
  }
  
}
```