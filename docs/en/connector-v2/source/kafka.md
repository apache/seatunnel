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

| Datasource | Supported Versions |                                                    Maven                                                    |
|------------|--------------------|-------------------------------------------------------------------------------------------------------------|
| Kafka      | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-kafka) |

## Source Options

|                Name                 |                                    Type                                     | Required |         Default          |                                                                                                                                                                                                             Description                                                                                                                                                                                                             |
|-------------------------------------|-----------------------------------------------------------------------------|----------|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                               | String                                                                      | Yes      | -                        | Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by comma like 'topic-1,topic-2'.                                                                                                                                                                                                                                                                       |
| bootstrap.servers                   | String                                                                      | Yes      | -                        | Comma separated list of Kafka brokers.                                                                                                                                                                                                                                                                                                                                                                                              |
| pattern                             | Boolean                                                                     | No       | false                    | If `pattern` is set to `true`,the regular expression for a pattern of topic names to read from. All topics in clients with names that match the specified regular expression will be subscribed by the consumer.                                                                                                                                                                                                                    |
| consumer.group                      | String                                                                      | No       | SeaTunnel-Consumer-Group | `Kafka consumer group id`, used to distinguish different consumer groups.                                                                                                                                                                                                                                                                                                                                                           |
| commit_on_checkpoint                | Boolean                                                                     | No       | true                     | If true the consumer's offset will be periodically committed in the background.                                                                                                                                                                                                                                                                                                                                                     |
| kafka.config                        | Map                                                                         | No       | -                        | In addition to the above necessary parameters that must be specified by the `Kafka consumer` client, users can also specify multiple `consumer` client non-mandatory parameters, covering [all consumer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#consumerconfigs).                                                                                                          |
| schema                              | Config                                                                      | No       | -                        | The structure of the data, including field names and field types.                                                                                                                                                                                                                                                                                                                                                                   |
| format                              | String                                                                      | No       | json                     | Data format. The default format is json. Optional text format, canal-json and debezium-json.If you use json or text format. The default field separator is ", ". If you customize the delimiter, add the "field_delimiter" option.If you use canal format, please refer to [canal-json](../formats/canal-json.md) for details.If you use debezium format, please refer to [debezium-json](../formats/debezium-json.md) for details. |
| format_error_handle_way             | String                                                                      | No       | fail                     | The processing method of data format error. The default value is fail, and the optional value is (fail, skip). When fail is selected, data format error will block and an exception will be thrown. When skip is selected, data format error will skip this line data.                                                                                                                                                              |
| field_delimiter                     | String                                                                      | No       | ,                        | Customize the field delimiter for data format.                                                                                                                                                                                                                                                                                                                                                                                      |
| start_mode                          | StartMode[earliest],[group_offsets],[latest],[specific_offsets],[timestamp] | No       | group_offsets            | The initial consumption pattern of consumers.                                                                                                                                                                                                                                                                                                                                                                                       |
| start_mode.offsets                  | Config                                                                      | No       | -                        | The offset required for consumption mode to be specific_offsets.                                                                                                                                                                                                                                                                                                                                                                    |
| start_mode.timestamp                | Long                                                                        | No       | -                        | The time required for consumption mode to be "timestamp".                                                                                                                                                                                                                                                                                                                                                                           |
| partition-discovery.interval-millis | Long                                                                        | No       | -1                       | The interval for dynamically discovering topics and partitions.                                                                                                                                                                                                                                                                                                                                                                     |
| common-options                      |                                                                             | No       | -                        | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                                                                                                                                                                                             |

## Task Example

### Simple

> This example reads the data of kafka's topic_1, topic_2, topic_3 and prints it to the client.And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in Install SeaTunnel to install and deploy SeaTunnel. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```hocon
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 2
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

