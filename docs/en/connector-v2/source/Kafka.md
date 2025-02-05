# Kafka

> Kafka source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Source connector for Apache Kafka.

## Supported DataSource Info

In order to use the Kafka connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Maven                                                                               |
|------------|--------------------|-------------------------------------------------------------------------------------|
| Kafka      | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-kafka) |

## Source Options

| Name                                | Type                                                                        | Required | Default                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|-------------------------------------|-----------------------------------------------------------------------------|----------|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                               | String                                                                      | Yes      | -                        | Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by comma like 'topic-1,topic-2'.                                                                                                                                                                                                                                                                                                                                                                       |
| table_list                          | Map                                                                         | No       | -                        | Topic list config You can configure only one `table_list` and one `topic` at the same time                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| bootstrap.servers                   | String                                                                      | Yes      | -                        | Comma separated list of Kafka brokers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| pattern                             | Boolean                                                                     | No       | false                    | If `pattern` is set to `true`,the regular expression for a pattern of topic names to read from. All topics in clients with names that match the specified regular expression will be subscribed by the consumer.                                                                                                                                                                                                                                                                                                                    |
| consumer.group                      | String                                                                      | No       | SeaTunnel-Consumer-Group | `Kafka consumer group id`, used to distinguish different consumer groups.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| commit_on_checkpoint                | Boolean                                                                     | No       | true                     | If true the consumer's offset will be periodically committed in the background.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| poll.timeout                        | Long                                                                        | No       | 10000                    | The interval(millis) for poll messages.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| kafka.config                        | Map                                                                         | No       | -                        | In addition to the above necessary parameters that must be specified by the `Kafka consumer` client, users can also specify multiple `consumer` client non-mandatory parameters, covering [all consumer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#consumerconfigs).                                                                                                                                                                                                          |
| schema                              | Config                                                                      | No       | -                        | The structure of the data, including field names and field types.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| format                              | String                                                                      | No       | json                     | Data format. The default format is json. Optional text format, canal_json, debezium_json, maxwell_json, ogg_json, avro and protobuf. If you use json or text format. The default field separator is ", ". If you customize the delimiter, add the "field_delimiter" option.If you use canal format, please refer to [canal-json](../formats/canal-json.md) for details.If you use debezium format, please refer to [debezium-json](../formats/debezium-json.md) for details. Some format details please refer [formats](../formats) |
| format_error_handle_way             | String                                                                      | No       | fail                     | The processing method of data format error. The default value is fail, and the optional value is (fail, skip). When fail is selected, data format error will block and an exception will be thrown. When skip is selected, data format error will skip this line data.                                                                                                                                                                                                                                                              |
| debezium_record_table_filter        | Config                                                                      | No       | -                        | Used for filtering data in debezium format, only when the format is set to `debezium_json`. Please refer `debezium_record_table_filter` below                                                                                                                                                                                                                                                                                                                                                                                       |
| field_delimiter                     | String                                                                      | No       | ,                        | Customize the field delimiter for data format.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| start_mode                          | StartMode[earliest],[group_offsets],[latest],[specific_offsets],[timestamp] | No       | group_offsets            | The initial consumption pattern of consumers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start_mode.offsets                  | Config                                                                      | No       | -                        | The offset required for consumption mode to be specific_offsets.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| start_mode.timestamp                | Long                                                                        | No       | -                        | The time required for consumption mode to be "timestamp".                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| partition-discovery.interval-millis | Long                                                                        | No       | -1                       | The interval for dynamically discovering topics and partitions.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| common-options                      |                                                                             | No       | -                        | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                   |
| protobuf_message_name               | String                                                                      | No       | -                        | Effective when the format is set to protobuf, specifies the Message name                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| protobuf_schema                     | String                                                                      | No       | -                        | Effective when the format is set to protobuf, specifies the Schema definition                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

### debezium_record_table_filter

We can use `debezium_record_table_filter` to filter the data in the debezium format. The configuration is as follows:

```hocon
debezium_record_table_filter {
  database_name = "test" // null if not exists
  schema_name = "public" // null if not exists
  table_name = "products"
}
```

Only the data of the `test.public.products` table will be consumed.

## Task Example

### Simple

> This example reads the data of kafka's topic_1, topic_2, topic_3 and prints it to the client.And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in Install SeaTunnel to install and deploy SeaTunnel. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.
> In batch mode, during the enumerator sharding process, it will fetch the latest offset for each partition and use it as the stopping point.

```hocon
# Defining the runtime environment
env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
  Kafka {
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
    format = text
    field_delimiter = "#"
    topic = "topic_1,topic_2,topic_3"
    bootstrap.servers = "localhost:9092"
    kafka.config = {
      client.id = client_1
      max.poll.records = 500
      auto.offset.reset = "earliest"
      enable.auto.commit = "false"
    }
  }  
}
sink {
  Console {}
}
```

### Regex Topic

```hocon
source {
    Kafka {
          topic = ".*seatunnel*."
          pattern = "true" 
          bootstrap.servers = "localhost:9092"
          consumer.group = "seatunnel_group"
    }
}
```

### AWS MSK SASL/SCRAM

Replace the following `${username}` and `${password}` with the configuration values in AWS MSK.

```hocon
source {
    Kafka {
        topic = "seatunnel"
        bootstrap.servers = "xx.amazonaws.com.cn:9096,xxx.amazonaws.com.cn:9096,xxxx.amazonaws.com.cn:9096"
        consumer.group = "seatunnel_group"
        kafka.config = {
            security.protocol=SASL_SSL
            sasl.mechanism=SCRAM-SHA-512
            sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";"
            #security.protocol=SASL_SSL
            #sasl.mechanism=AWS_MSK_IAM
            #sasl.jaas.config="software.amazon.msk.auth.iam.IAMLoginModule required;"
            #sasl.client.callback.handler.class="software.amazon.msk.auth.iam.IAMClientCallbackHandler"
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

Source Config

```hocon
source {
    Kafka {
        topic = "seatunnel"
        bootstrap.servers = "xx.amazonaws.com.cn:9098,xxx.amazonaws.com.cn:9098,xxxx.amazonaws.com.cn:9098"
        consumer.group = "seatunnel_group"
        kafka.config = {
            #security.protocol=SASL_SSL
            #sasl.mechanism=SCRAM-SHA-512
            #sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";"
            security.protocol=SASL_SSL
            sasl.mechanism=AWS_MSK_IAM
            sasl.jaas.config="software.amazon.msk.auth.iam.IAMLoginModule required;"
            sasl.client.callback.handler.class="software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        }
    }
}
```

### Kerberos Authentication Example

Source Config

```
source {
    Kafka {
        topic = "seatunnel"
        bootstrap.servers = "127.0.0.1:9092"
        consumer.group = "seatunnel_group"
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

### Multiple Kafka Source

> This is written to the same pg table according to different formats and topics of parsing kafka Perform upsert operations based on the id

> Note: Kafka is an unstructured data source and should be use 'tables_configs', and 'table_list' will be removed in the future.

```hocon

env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafka_e2e:9092"
    tables_configs = [
      {
        topic = "^test-ogg-sou.*"
        pattern = "true"
        consumer.group = "ogg_multi_group"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = ogg_json
      },
      {
        topic = "test-cdc_mds"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = canal_json
      }
    ]
  }
}

sink {
  Jdbc {
    driver = org.postgresql.Driver
    url = "jdbc:postgresql://postgresql:5432/test?loggerLevel=OFF"
    user = test
    password = test
    generate_sink_sql = true
    database = test
    table = public.sink
    primary_keys = ["id"]
  }
}
```

```hocon

env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafka_e2e:9092"
    table_list = [
      {
        topic = "^test-ogg-sou.*"
        pattern = "true"
        consumer.group = "ogg_multi_group"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = ogg_json
      },
      {
        topic = "test-cdc_mds"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = canal_json
      }
    ]
  }
}

sink {
  Jdbc {
    driver = org.postgresql.Driver
    url = "jdbc:postgresql://postgresql:5432/test?loggerLevel=OFF"
    user = test
    password = test
    generate_sink_sql = true
    database = test
    table = public.sink
    primary_keys = ["id"]
  }
}
```

### Protobuf configuration

Set `format` to `protobuf`, configure `protobuf` data structure, `protobuf_message_name` and `protobuf_schema` parameters

Example:

```hocon
source {
  Kafka {
    topic = "test_protobuf_topic_fake_source"
    format = protobuf
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
    bootstrap.servers = "kafkaCluster:9092"
    start_mode = "earliest"
    plugin_output = "kafka_table"
  }
}
```
