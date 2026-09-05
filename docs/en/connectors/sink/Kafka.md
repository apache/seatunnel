import ChangeLog from '../changelog/connector-kafka.md';

# Kafka

> Kafka sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

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

| Name                  | Type   | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|-----------------------|--------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                 | String | Yes      | -       | When the table is used as sink, the topic name is the topic to write data to.                                                                                                                                                                                                                                                                                                                                                                                |
| bootstrap.servers     | String | Yes      | -       | Comma separated list of Kafka brokers.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| kafka.config          | Map    | No       | -       | In addition to the above parameters that must be specified by the `Kafka producer` client, the user can also specify multiple non-mandatory parameters for the `producer` client, covering [all the producer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#producerconfigs).                                                                                                                              |
| semantics             | String | No       | NON     | Semantics that can be chosen EXACTLY_ONCE/AT_LEAST_ONCE/NON, default NON.                                                                                                                                                                                                                                                                                                                                                                                    |
| partition_key_fields  | Array  | No       | -       | Configure which fields are used as the key of the kafka message.                                                                                                                                                                                                                                                                                                                                                                                             |
| kafka_headers_fields  | Array  | No       | -       | Configure which fields are used as the headers of the kafka message. The field value will be converted to a string and used as the header value.                                                                                                                                                                                                                                                                                                             |
| partition             | Int    | No       | -       | We can specify the partition, all messages will be sent to this partition.                                                                                                                                                                                                                                                                                                                                                                                   |
| assign_partitions     | Array  | No       | -       | We can decide which partition to send based on the content of the message. The function of this parameter is to distribute information.                                                                                                                                                                                                                                                                                                                      |
| transaction_prefix    | String | No       | -       | If `semantics` is `EXACTLY_ONCE`, the producer writes messages in Kafka transactions. Kafka distinguishes transactions by transaction id, so use a different prefix for each job.                                                                                                                                                                                                               |
| format                | String | No       | json    | Data format. The default format is json. Optional text, canal_json, debezium_json, compatible_debezium_json, ogg_json, maxwell_json, avro, protobuf and native. If you use json or text format. The default field separator is ", ". If you customize the delimiter, add the "field_delimiter" option.If you use canal format, please refer to [canal-json](../formats/canal-json.md) for details.If you use debezium format, please refer to [debezium-json](../formats/debezium-json.md) for details. |
| field_delimiter       | String | No       | ,       | Customize the field delimiter for data format.                                                                                                                                                                                                                                                                                                                                                                                                               |
| common-options        |        | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details                                                                                                                                                                                                                                                                                                                                              |
| protobuf_message_name | String | No       | -       | Effective when the format is set to protobuf, specifies the Message name                                                                                                                                                                                                                                                                                                                                                                                     |
| protobuf_schema       | String | No       | -       | Effective when the format is set to protobuf, specifies the Schema definition                                                                                                                                                                                                                                                                                                                                                                                |


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

For `EXACTLY_ONCE`, enable checkpoints and make sure every running job uses a unique `transaction_prefix`. Reusing the same transaction prefix across jobs can cause Kafka transaction conflicts.

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

### Kafka Headers Fields

For example, if you want to use value of fields from upstream data as kafka message headers, you can assign field names to this property.

Upstream data is the following:

| name | age |     data      | source | traceId   |
|------|-----|---------------|--------|-----------|
| Jack | 16  | data-example1 | web    | trace-123 |
| Mary | 23  | data-example2 | mobile | trace-456 |

If source and traceId are set as the kafka headers fields, then these field values will be added as headers to the kafka message.
For example, the first row will have headers: `source=web` and `traceId=trace-123`.
The field values will be converted to strings and used as header values.
The selected fields must be existing fields in the upstream.

Note:
Fields configured as Kafka headers will be excluded from the message value (payload) and will only be present in the Kafka message headers.
`kafka_headers_fields` is not supported when `format = native`.

### Assign Partitions

For example, there are five partitions in total, and the assign_partitions field in config is as follows:
assign_partitions = ["shoe", "clothing"]
Then the message containing "shoe" will be sent to partition zero ,because "shoe" is subscribed as zero in assign_partitions, and the message containing "clothing" will be sent to partition one.For other messages, the hash algorithm will be used to divide them into the remaining partitions.
This function by `MessageContentPartitioner` class implements `org.apache.kafka.clients.producer.Partitioner` interface.If we need custom partitions, we need to implement this interface as well.

## Task Example

### Simple

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to Kafka Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target topic is test_topic will also be 16 rows of data in the topic. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../getting-started/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../getting-started/locally/quick-start-seatunnel-engine.md) to run this job.

```hocon
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
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
      semantics = EXACTLY_ONCE
      kafka.config = {
        acks = "all"
        request.timeout.ms = 60000
        buffer.memory = 33554432
      }
  }
}
```

### Using Kafka Headers

This example shows how to use kafka_headers_fields to set Kafka message headers:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
        source = "string"
        traceId = "string"
      }
    }
  }
}

sink {
  kafka {
      topic = "test_topic"
      bootstrap.servers = "localhost:9092"
      format = json
      partition_key_fields = ["name"]
      kafka_headers_fields = ["source", "traceId"]
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

Please set JVM parameters `java.security.krb5.conf` before starting the SeaTunnel or update default `krb5.conf` in `/etc/krb5.conf`.

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


### format
If you need to write Kafka's native information, you can refer to the following configuration.

Config Example:
```hocon
sink {
  kafka {
      topic = "test_topic_native_sink"
      bootstrap.servers = "kafkaCluster:9092"
      format = "NATIVE"
  }
}
```

The input parameter requirements are as follows:
```json
{
  "headers": {
    "header1": "header1",
    "header2": "header2"
  },
  "key": "dGVzdF9ieXRlc19kYXRh",
  "partition": 3,
  "timestamp": 1672531200000,
  "timestampType": "CREATE_TIME",
  "value": "dGVzdF9ieXRlc19kYXRh"
}
```
Note：key/value is of type byte[].

### Streaming EXACTLY_ONCE With Checkpoints

For long-running streaming jobs that must not lose or duplicate messages on restart, configure `semantics = EXACTLY_ONCE` and enable checkpointing. SeaTunnel coordinates Kafka transactions with checkpoints so each in-flight batch is committed atomically with the corresponding consumer offset.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Kafka {
    topic = "orders"
    bootstrap.servers = "localhost:9092"
    consumer.group = "orders_consumer"
    start_mode = group_offsets
    format = json
    schema = {
      fields {
        order_id = bigint
        user_id = bigint
        amount = double
      }
    }
  }
}

sink {
  Kafka {
    topic = "orders_sink"
    bootstrap.servers = "localhost:9092"
    format = json
    semantics = EXACTLY_ONCE
    transaction_prefix = "orders_pipeline"
    kafka.config = {
      "transaction.timeout.ms" = "900000"
    }
    partition_key_fields = ["order_id"]
  }
}
```

Important: pick a unique `transaction_prefix` per job. Kafka distinguishes transactions by the transactional id, and reusing the same prefix across concurrent jobs causes Kafka transaction conflicts.

### Round-Trip Headers With NATIVE Format

When the source reads with `format = "NATIVE"` and writes with `format = "NATIVE"`, the headers, key, partition and timestamp fields round-trip back into Kafka as-is. This is useful when forwarding already-Kafka-encoded records between topics without changing their on-the-wire layout.

```hocon
source {
  Kafka {
    topic = "topic_native_source"
    bootstrap.servers = "localhost:9092"
    format = "NATIVE"
    consumer.group = "native_forwarder"
  }
}

sink {
  Kafka {
    topic = "topic_native_sink"
    bootstrap.servers = "localhost:9092"
    format = "NATIVE"
  }
}
```

Note: when the upstream rows are produced with `format = "NATIVE"`, the `key` and `value` columns are `byte[]`. Combining `kafka_headers_fields` with `format = "NATIVE"` is not a "configure carefully" situation — `KafkaSinkWriter.getSerializer()` throws `KafkaConnectorException(OPERATION_NOT_SUPPORTED)` at job initialization if both are set, so the job fails to start. Do not configure `kafka_headers_fields` together with `format = "NATIVE"`; headers for NATIVE inputs are already encoded inside the `value` byte array.

## FAQ

### Does Kafka Sink automatically create topics?

SeaTunnel Kafka Sink writes records to the configured `topic` but does not explicitly create Kafka topics itself. Whether a missing topic is created automatically depends on the Kafka broker's `auto.create.topics.enable` setting.

In production, we recommend creating topics explicitly in advance to control partition count, replication factor, retention policy, and ACLs. Do not rely on automatic topic creation for production workloads, as brokers may have `auto.create.topics.enable = false`.

### What happens if `partition_key_fields` is not configured?

If `partition_key_fields` is not set, SeaTunnel sends records with a **null** Kafka message key. Kafka then distributes records across partitions using its default round-robin strategy.

This is suitable for load distribution but does **not** preserve ordering for records with the same business key. If you need records with the same business key to land in the same partition, configure `partition_key_fields` with the relevant field names.

### How do I achieve exactly-once delivery to Kafka?

Set `semantics = EXACTLY_ONCE` to enable exactly-once delivery, and configure `transaction_prefix`
so each job uses a distinct Kafka transactional ID prefix. SeaTunnel coordinates Kafka transactions
with checkpoints to provide exactly-once semantics:

```hocon
sink {
  kafka {
    topic = "output-topic"
    bootstrap.servers = "localhost:9092"
    semantics = EXACTLY_ONCE
    transaction_prefix = "SeaTunnelJob"
    kafka.config = {
      "transaction.timeout.ms" = "900000"
    }
  }
}
```

Ensure the Kafka broker has transactions enabled and that `transaction.timeout.ms` is aligned with your checkpoint interval.

Under `EXACTLY_ONCE`, a failed send fails the checkpoint instead of silently dropping records. Two
errors can be reported in that situation:

| Code     | Name                    | Meaning                                                                       | What to do                                                                                                   |
|----------|-------------------------|-------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| KAFKA-08 | TRANSACTION_NOT_STARTED | The transaction carries records but Kafka never registered it on the broker.   | Check broker availability and whether `transaction.timeout.ms` is shorter than the checkpoint interval.       |
| KAFKA-09 | PRODUCE_DATA_FAILED     | A record of the transaction failed to be sent asynchronously.                  | Read the exception cause; retriable causes usually recover on checkpoint retry, others need broker-side work. |

Both errors abort the current transaction, so the affected records are re-sent from the last
completed checkpoint rather than lost.

### How do I configure SASL/Kerberos authentication?

Pass broker authentication settings via `kafka.*` properties:

```hocon
sink {
  kafka {
    topic = "secure-topic"
    bootstrap.servers = "broker:9092"
    kafka.security.protocol = "SASL_PLAINTEXT"
    kafka.sasl.mechanism = "GSSAPI"
    kafka.sasl.kerberos.service.name = "kafka"
    kafka.sasl.jaas.config = """com.sun.security.auth.module.Krb5LoginModule required
      useKeyTab=true
      keyTab="/etc/kafka/kafka.keytab"
      principal="user@REALM.COM";"""
  }
}
```

### What message formats does Kafka Sink support?

Kafka Sink supports: `json`, `text`, `canal_json`, `debezium_json`, `ogg_json`, `avro`, `protobuf`, and `NATIVE`. Use `NATIVE` when the upstream data is already in Kafka-native format (with headers, key, and value as byte fields).

## Changelog

<ChangeLog />
