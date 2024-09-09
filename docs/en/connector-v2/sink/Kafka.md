# Kafka

> Kafka sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

> By default, we will use 2pc to guarantee the message is sent to kafka exactly once.

## Description

Write Rows to a Kafka topic.

## Supported DataSource Info

In order to use the Kafka connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Maven                                                                               |
|------------|--------------------|-------------------------------------------------------------------------------------|
| Kafka      | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-kafka) |

## Sink Options

| Name                  | Type   | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|-----------------------|--------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                 | String | Yes      | -       | When the table is used as sink, the topic name is the topic to write data to.                                                                                                                                                                                                                                                                                                                                                                       |
| bootstrap.servers     | String | Yes      | -       | Comma separated list of Kafka brokers.                                                                                                                                                                                                                                                                                                                                                                                                              |
| kafka.config          | Map    | No       | -       | In addition to the above parameters that must be specified by the `Kafka producer` client, the user can also specify multiple non-mandatory parameters for the `producer` client, covering [all the producer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#producerconfigs).                                                                                                                     |
| semantics             | String | No       | NON     | Semantics that can be chosen EXACTLY_ONCE/AT_LEAST_ONCE/NON, default NON.                                                                                                                                                                                                                                                                                                                                                                           |
| partition_key_fields  | Array  | No       | -       | Configure which fields are used as the key of the kafka message.                                                                                                                                                                                                                                                                                                                                                                                    |
| partition             | Int    | No       | -       | We can specify the partition, all messages will be sent to this partition.                                                                                                                                                                                                                                                                                                                                                                          |
| assign_partitions     | Array  | No       | -       | We can decide which partition to send based on the content of the message. The function of this parameter is to distribute information.                                                                                                                                                                                                                                                                                                             |
| transaction_prefix    | String | No       | -       | If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a Kafka transaction,kafka distinguishes different transactions by different transactionId. This parameter is prefix of  kafka  transactionId, make sure different job use different prefix.                                                                                                                                                                       |
| format                | String | No       | json    | Data format. The default format is json. Optional text format, canal_json, debezium_json, ogg_json and avro.If you use json or text format. The default field separator is ", ". If you customize the delimiter, add the "field_delimiter" option.If you use canal format, please refer to [canal-json](../formats/canal-json.md) for details.If you use debezium format, please refer to [debezium-json](../formats/debezium-json.md) for details. |
| field_delimiter       | String | No       | ,       | Customize the field delimiter for data format.                                                                                                                                                                                                                                                                                                                                                                                                      |
| common-options        |        | No       | -       | Source plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details                                                                                                                                                                                                                                                                                                                                     |
| protobuf_message_name | String | No       | -       | Effective when the format is set to protobuf, specifies the Message name                                                                                                                                                                                                                                                                                                                                                                            |
| protobuf_schema       | String | No       | -       | Effective when the format is set to protobuf, specifies the Schema definition                                                                                                                                                                                                                                                                                                                                                                       |


## Parameter Interpretation

### Topic Formats

Currently two formats are supported:

1. Fill in the name of the topic.

2. Use value of a field from upstream data as topic,the format is `${your field name}`, where topic is the value of one of the columns of the upstream data.

   For example, Upstream data is the following:

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

If `${name}` is set as the topic. So the first row is sent to Jack topic, and the second row is sent to Mary topic.

### Semantics

In EXACTLY_ONCE, producer will write all messages in a Kafka transaction that will be committed to Kafka on a checkpoint.
In AT_LEAST_ONCE, producer will wait for all outstanding messages in the Kafka buffers to be acknowledged by the Kafka producer on a checkpoint.
NON does not provide any guarantees: messages may be lost in case of issues on the Kafka broker and messages may be duplicated.

### Partition Key Fields

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

### Assign Partitions

For example, there are five partitions in total, and the assign_partitions field in config is as follows:
assign_partitions = ["shoe", "clothing"]
Then the message containing "shoe" will be sent to partition zero ,because "shoe" is subscribed as zero in assign_partitions, and the message containing "clothing" will be sent to partition one.For other messages, the hash algorithm will be used to divide them into the remaining partitions.
This function by `MessageContentPartitioner` class implements `org.apache.kafka.clients.producer.Partitioner` interface.If we need custom partitions, we need to implement this interface as well.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to Kafka Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target topic is test_topic will also be 16 rows of data in the topic. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```hocon
# Defining the runtime environment
env {
  parallelism = 1
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
  kafka {
      topic = "test_topic"
      bootstrap.servers = "localhost:9092"
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

### Kerberos Authentication Example

Sink Config

```
sink {
    Kafka {
        topic = "seatunnel"
        bootstrap.servers = "127.0.0.1:9092"
        format = json
        semantics = EXACTLY_ONCE
        kafka.config = {
            security.protocol=SASL_PLAINTEXT
            sasl.kerberos.service.name=kafka
            sasl.mechanism=GSSAPI
            java.security.krb5.conf="/etc/krb5.conf"
            sasl.jaas.config="com.sun.security.auth.module.Krb5LoginModule required \n        useKeyTab=true \n        storeKey=true  \n        keyTab=\"/path/to/xxx.keytab\" \n        principal=\"user@xxx.com\";"
        }
    }
}
```


### Protobuf Configuration

Set the `format` to `protobuf` and configure the `protobuf` data structure using the `protobuf_message_name` and `protobuf_schema` parameters.

Example Usage:

```hocon
sink {
  kafka {
      topic = "test_protobuf_topic_fake_source"
      bootstrap.servers = "kafkaCluster:9092"
      format = protobuf
      kafka.request.timeout.ms = 60000
      kafka.config = {
        acks = "all"
        request.timeout.ms = 60000
        buffer.memory = 33554432
      }
      protobuf_message_name = Person
      protobuf_schema = """
              syntax = "proto3";

              package org.apache.seatunnel.format.protobuf;

              option java_outer_classname = "ProtobufE2E";

              message Person {
                int32 c_int32 = 1;
                int64 c_int64 = 2;
                float c_float = 3;
                double c_double = 4;
                bool c_bool = 5;
                string c_string = 6;
                bytes c_bytes = 7;

                message Address {
                  string street = 1;
                  string city = 2;
                  string state = 3;
                  string zip = 4;
                }

                Address address = 8;

                map<string, float> attributes = 9;

                repeated string phone_numbers = 10;
              }
              """
  }
}
```
