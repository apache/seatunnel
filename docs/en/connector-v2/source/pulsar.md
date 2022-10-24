# Apache Pulsar

> Apache Pulsar source connector

## Description

Source connector for Apache Pulsar.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                     | type    | required | default value |
|--------------------------|---------|----------|---------------|
| topic                    | String  | No       | -             |
| topic-pattern            | String  | No       | -             |
| topic-discovery.interval | Long    | No       | -1            |
| subscription.name        | String  | Yes      | -             |
| client.service-url       | String  | Yes      | -             |
| admin.service-url        | String  | Yes      | -             |
| auth.plugin-class        | String  | No       | -             |
| auth.params              | String  | No       | -             |
| poll.timeout             | Integer | No       | 100           |
| poll.interval            | Long    | No       | 50            |
| poll.batch.size          | Integer | No       | 500           |
| cursor.startup.mode      | Enum    | No       | LATEST        |
| cursor.startup.timestamp | Long    | No       | -             |
| cursor.reset.mode        | Enum    | No       | LATEST        |
| cursor.stop.mode         | Enum    | No       | NEVER         |
| cursor.stop.timestamp    | Long    | No       | -             |
| schema                   | config  | No       | -             |
| common-options           |         | no       | -             |

### topic [String]

Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'.

**Note, only one of "topic-pattern" and "topic" can be specified for sources.**

### topic-pattern [String]

The regular expression for a pattern of topic names to read from. All topics with names that match the specified regular expression will be subscribed by the consumer when the job starts running.

**Note, only one of "topic-pattern" and "topic" can be specified for sources.**

### topic-discovery.interval [Long]

The interval (in ms) for the Pulsar source to discover the new topic partitions. A non-positive value disables the topic partition discovery.

**Note, This option only works if the 'topic-pattern' option is used.**

### subscription.name [String]

Specify the subscription name for this consumer. This argument is required when constructing the consumer.

### client.service-url [String]

Service URL provider for Pulsar service.
To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.
You can assign Pulsar protocol URLs to specific clusters and use the Pulsar scheme.

For example, `localhost`: `pulsar://localhost:6650,localhost:6651`.

### admin.service-url [String]

The Pulsar service HTTP URL for the admin endpoint.

For example, `http://my-broker.example.com:8080`, or `https://my-broker.example.com:8443` for TLS.

### auth.plugin-class [String]

Name of the authentication plugin.

### auth.params [String]

Parameters for the authentication plugin.

For example, `key1:val1,key2:val2`

### poll.timeout [Integer]

The maximum time (in ms) to wait when fetching records. A longer time increases throughput but also latency.

### poll.interval [Long]

The interval time(in ms) when fetcing records. A shorter time increases throughput, but also increases CPU load.

### poll.batch.size [Integer]

The maximum number of records to fetch to wait when polling. A longer time increases throughput but also latency.

### cursor.startup.mode [Enum]

Startup mode for Pulsar consumer, valid values are `'EARLIEST'`, `'LATEST'`, `'SUBSCRIPTION'`, `'TIMESTAMP'`.

### cursor.startup.timestamp [String]

Start from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "cursor.startup.mode" option used `'TIMESTAMP'`.**

### cursor.reset.mode [Enum]

Cursor reset strategy for Pulsar consumer valid values are `'EARLIEST'`, `'LATEST'`.

**Note, This option only works if the "cursor.startup.mode" option used `'SUBSCRIPTION'`.**

### cursor.stop.mode [String]

Stop mode for Pulsar consumer, valid values are `'NEVER'`, `'LATEST'`and `'TIMESTAMP'`.

**Note, When `'NEVER' `is specified, it is a real-time job, and other mode are off-line jobs.**

### cursor.startup.timestamp [String]

Stop from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "cursor.stop.mode" option used `'TIMESTAMP'`.**

### schema [Config]

#### fields [Config]

the schema fields of upstream data.

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

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

## Changelog

### 2.3.0-beta 2022-10-20
- Add Pulsar Source Connector
