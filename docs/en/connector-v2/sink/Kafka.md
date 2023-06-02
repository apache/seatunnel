# Kafka

> Kafka sink connector
>
  ## Description

Write Rows to a Kafka topic.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we will use 2pc to guarantee the message is sent to kafka exactly once.

## Options

|         name         |  type  | required | default value |
|----------------------|--------|----------|---------------|
| topic                | string | yes      | -             |
| bootstrap.servers    | string | yes      | -             |
| kafka.config         | map    | no       | -             |
| semantics            | string | no       | NON           |
| partition_key_fields | array  | no       | -             |
| partition            | int    | no       | -             |
| assign_partitions    | array  | no       | -             |
| transaction_prefix   | string | no       | -             |
| format               | String | no       | json          |
| field_delimiter      | String | no       | ,             |
| common-options       | config | no       | -             |

### topic [string]

Kafka Topic.

Currently two formats are supported:

1. Fill in the name of the topic.

2. Use value of a field from upstream data as topic,the format is `${your field name}`, where topic is the value of one of the columns of the upstream data.

   For example, Upstream data is the following:

   | name | age |     data      |
   |------|-----|---------------|
   | Jack | 16  | data-example1 |
   | Mary | 23  | data-example2 |

   If `${name}` is set as the topic. So the first row is sent to Jack topic, and the second row is sent to Mary topic.

### bootstrap.servers [string]

Kafka Brokers List.

### kafka.config [kafka producer config]

In addition to the above parameters that must be specified by the `Kafka producer` client, the user can also specify multiple non-mandatory parameters for the `producer` client, covering [all the producer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#producerconfigs).

### semantics [string]

Semantics that can be chosen EXACTLY_ONCE/AT_LEAST_ONCE/NON, default NON.

In EXACTLY_ONCE, producer will write all messages in a Kafka transaction that will be committed to Kafka on a checkpoint.

In AT_LEAST_ONCE, producer will wait for all outstanding messages in the Kafka buffers to be acknowledged by the Kafka producer on a checkpoint.

NON does not provide any guarantees: messages may be lost in case of issues on the Kafka broker and messages may be duplicated.

### partition_key_fields [array]

Configure which fields are used as the key of the kafka message.

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

### partition [int]

We can specify the partition, all messages will be sent to this partition.

### assign_partitions [array]

We can decide which partition to send based on the content of the message. The function of this parameter is to distribute information.

For example, there are five partitions in total, and the assign_partitions field in config is as follows:
assign_partitions = ["shoe", "clothing"]

Then the message containing "shoe" will be sent to partition zero ,because "shoe" is subscribed as zero in assign_partitions, and the message containing "clothing" will be sent to partition one.For other messages, the hash algorithm will be used to divide them into the remaining partitions.

This function by `MessageContentPartitioner` class implements `org.apache.kafka.clients.producer.Partitioner` interface.If we need custom partitions, we need to implement this interface as well.

### transaction_prefix [string]

If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a Kafka transaction.
Kafka distinguishes different transactions by different transactionId. This parameter is prefix of  kafka  transactionId, make sure different job use different prefix.

### format

Data format. The default format is json. Optional text format. The default field separator is ",".
If you customize the delimiter, add the "field_delimiter" option.

### field_delimiter

Customize the field delimiter for data format.

### common options [config]

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Examples

```hocon
sink {

  kafka {
      topic = "seatunnel"
      bootstrap.servers = "localhost:9092"
      partition = 3
      format = json
      kafka.request.timeout.ms = 60000
      semantics = EXACTLY_ONCE
      kafka.config = {
        acks = "all"
        request.timeout.ms = 60000
        buffer.memory = 33554432
      }
  }
  
}
```

### AWS MSK SASL/SCRAM

Replace the following `${username}` and `${password}` with the configuration values in AWS MSK.

```hocon
sink {
  kafka {
      topic = "seatunnel"
      bootstrap.servers = "localhost:9092"
      partition = 3
      format = json
      kafka.request.timeout.ms = 60000
      semantics = EXACTLY_ONCE
      kafka.config = {
         security.protocol=SASL_SSL
         sasl.mechanism=SCRAM-SHA-512
         sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required \nusername=${username}\npassword=${password};"
      }
  }
  
}
```

### AWS MSK IAM

Download `aws-msk-iam-auth-1.1.5.jar` from https://github.com/aws/aws-msk-iam-auth/releases and put it in `$SEATUNNEL_HOME/plugin/kafka/lib` dir.

Please ensure the IAM policy have `"kafka-cluster:Connect",`. Like this:

```hocon
"Effect": "Allow",
"Action": [
    "kafka-cluster:Connect",
    "kafka-cluster:AlterCluster",
    "kafka-cluster:DescribeCluster"
],
```

Sink Config

```hocon
sink {
  kafka {
      topic = "seatunnel"
      bootstrap.servers = "localhost:9092"
      partition = 3
      format = json
      kafka.request.timeout.ms = 60000
      semantics = EXACTLY_ONCE
      kafka.config = {
         security.protocol=SASL_SSL
         sasl.mechanism=AWS_MSK_IAM
         sasl.jaas.config="software.amazon.msk.auth.iam.IAMLoginModule required;"
         sasl.client.callback.handler.class="software.amazon.msk.auth.iam.IAMClientCallbackHandler"
      }
  }
  
}
```

## Changelog

### 2.3.0-beta 2022-10-20

- Add Kafka Sink Connector

### next version

- [Improve] Support to specify multiple partition keys [3230](https://github.com/apache/seatunnel/pull/3230)
- [Improve] Add text format for kafka sink connector [3711](https://github.com/apache/seatunnel/pull/3711)
- [Improve] Support extract topic from SeaTunnelRow fields [3742](https://github.com/apache/seatunnel/pull/3742)
- [Improve] Change Connector Custom Config Prefix To Map [3719](https://github.com/apache/seatunnel/pull/3719)

