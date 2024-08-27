# Pulsar

> Pulsar sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

## Description

Sink connector for Apache Pulsar.

## Supported DataSource Info

| Datasource | Supported Versions |
|------------|--------------------|
| Pulsar     | Universal          |

## Sink Options

|         Name         |  Type  | Required |       Default       |                                                   Description                                                    |
|----------------------|--------|----------|---------------------|------------------------------------------------------------------------------------------------------------------|
| topic                | String | Yes      | -                   | sink pulsar topic                                                                                                |
| client.service-url   | String | Yes      | -                   | Service URL provider for Pulsar service.                                                                         |
| admin.service-url    | String | Yes      | -                   | The Pulsar service HTTP URL for the admin endpoint.                                                              |
| auth.plugin-class    | String | No       | -                   | Name of the authentication plugin.                                                                               |
| auth.params          | String | No       | -                   | Parameters for the authentication plugin.                                                                        |
| format               | String | No       | json                | Data format. The default format is json. Optional text format.                                                   |
| field_delimiter      | String | No       | ,                   | Customize the field delimiter for data format.                                                                   |
| semantics            | Enum   | No       | AT_LEAST_ONCE       | Consistency semantics for writing to pulsar.                                                                     |
| transaction_timeout  | Int    | No       | 600                 | The transaction timeout is specified as 10 minutes by default.                                                   |
| pulsar.config        | Map    | No       | -                   | In addition to the above parameters that must be specified by the Pulsar producer client.                        |
| message.routing.mode | Enum   | No       | RoundRobinPartition | Default routing mode for messages to partition.                                                                  |
| partition_key_fields | array  | No       | -                   | Configure which fields are used as the key of the pulsar message.                                                |
| common-options       | config | no       | -                   | Source plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details. |

## Parameter Interpretation

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

### format [String]

Data format. The default format is json. Optional text format. The default field separator is ",".
If you customize the delimiter, add the "field_delimiter" option.

### field_delimiter [String]

Customize the field delimiter for data format.The default field_delimiter is ','.

### semantics [Enum]

Consistency semantics for writing to pulsar.
Available options are EXACTLY_ONCE,NON,AT_LEAST_ONCE, default AT_LEAST_ONCE.
If semantic is specified as EXACTLY_ONCE, we will use 2pc to guarantee the message is sent to pulsar exactly once.
If semantic is specified as NON, we will directly send the message to pulsar, the data may duplicat/lost if
job restart/retry or network error.

### transaction_timeout [Int]

The transaction timeout is specified as 10 minutes by default.
If the transaction does not commit within the specified timeout, the transaction will be automatically aborted.
So you need to ensure that the timeout is greater than the checkpoint interval.

### pulsar.config [Map]

In addition to the above parameters that must be specified by the Pulsar producer client,
the user can also specify multiple non-mandatory parameters for the producer client,
covering all the producer parameters specified in the official Pulsar document.

### message.routing.mode [Enum]

Default routing mode for messages to partition.
Available options are SinglePartition,RoundRobinPartition.
If you choose SinglePartition, If no key is provided, The partitioned producer will randomly pick one single partition and publish all the messages into that partition, If a key is provided on the message, the partitioned producer will hash the key and assign message to a particular partition.
If you choose RoundRobinPartition, If no key is provided, the producer will publish messages across all partitions in round-robin fashion to achieve maximum throughput.
Please note that round-robin is not done per individual message but rather it's set to the same boundary of batching delay, to ensure batching is effective.

### partition_key_fields [String]

Configure which fields are used as the key of the pulsar message.

For example, if you want to use value of fields from upstream data as key, you can assign field names to this property.

Upstream data is the following:

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

If name is set as the key, then the hash value of the name column will determine which partition the message is sent to.

If not set partition key fields, the null message key will be sent to.

The format of the message key is json, If name is set as the key, for example '{"name":"Jack"}'.

The selected field must be an existing field in the upstream.

### common options

Source plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to Pulsar Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target topic is test_topic will also be 16 rows of data in the topic. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```hocon
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    result_table_name = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Pulsar {
  	topic = "example"
    client.service-url = "localhost:pulsar://localhost:6650"
    admin.service-url = "http://my-broker.example.com:8080"
    result_table_name = "test"
    pulsar.config = {
        sendTimeoutMs = 30000
    }
  }
}
```

## Changelog

### next version

- Add Pulsar Sink Connector

