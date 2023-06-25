# Kafka

> Kafka source connector

## Description

Source connector for Apache Kafka.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|                name                 |  type   | required |      default value       |
|-------------------------------------|---------|----------|--------------------------|
| topic                               | String  | yes      | -                        |
| bootstrap.servers                   | String  | yes      | -                        |
| pattern                             | Boolean | no       | false                    |
| consumer.group                      | String  | no       | SeaTunnel-Consumer-Group |
| commit_on_checkpoint                | Boolean | no       | true                     |
| kafka.config                        | Map     | no       | -                        |
| common-options                      | config  | no       | -                        |
| schema                              |         | no       | -                        |
| format                              | String  | no       | json                     |
| format_error_handle_way             | String  | no       | fail                     |
| field_delimiter                     | String  | no       | ,                        |
| start_mode                          | String  | no       | group_offsets            |
| start_mode.offsets                  |         | no       |                          |
| start_mode.timestamp                | Long    | no       |                          |
| partition-discovery.interval-millis | long    | no       | -1                       |

### topic [string]

`Kafka topic` name. If there are multiple `topics`, use `,` to split, for example: `"tpc1,tpc2"`.

### bootstrap.servers [string]

`Kafka` cluster address, separated by `","`.

### pattern [boolean]

If `pattern` is set to `true`,the regular expression for a pattern of topic names to read from. All topics in clients with names that match the specified regular expression will be subscribed by the consumer.

### consumer.group [string]

`Kafka consumer group id`, used to distinguish different consumer groups.

### commit_on_checkpoint [boolean]

If true the consumer's offset will be periodically committed in the background.

## partition-discovery.interval-millis [long]

The interval for dynamically discovering topics and partitions.

### kafka.config [map]

In addition to the above necessary parameters that must be specified by the `Kafka consumer` client, users can also specify multiple `consumer` client non-mandatory parameters, covering [all consumer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#consumerconfigs).

### common-options [config]

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

### schema

The structure of the data, including field names and field types.

## format

Data format. The default format is json. Optional text format. The default field separator is ", ".
If you customize the delimiter, add the "field_delimiter" option.

## format_error_handle_way

The processing method of data format error. The default value is fail, and the optional value is (fail, skip).
When fail is selected, data format error will block and an exception will be thrown.
When skip is selected, data format error will skip this line data.

## field_delimiter

Customize the field delimiter for data format.

## start_mode

The initial consumption pattern of consumers,there are several types:
[earliest],[group_offsets],[latest],[specific_offsets],[timestamp]

## start_mode.timestamp

The time required for consumption mode to be "timestamp".

## start_mode.offsets

The offset required for consumption mode to be specific_offsets.

for example:

```hocon
start_mode.offsets = {
         info-0 = 70
         info-1 = 10
         info-2 = 10
      }
```

## Example

### Simple

```hocon
source {

  Kafka {
    result_table_name = "kafka_name"
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
            sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required \nusername=${username}\npassword=${password};"
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
            #sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required \nusername=${username}\npassword=${password};"
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

- Add Kafka Source Connector

### Next Version

- [Improve] Support setting read starting offset or time at startup config ([3157](https://github.com/apache/seatunnel/pull/3157))
- [Improve] Support for dynamic discover topic & partition in streaming mode ([3125](https://github.com/apache/seatunnel/pull/3125))
- [Improve] Change Connector Custom Config Prefix To Map [3719](https://github.com/apache/seatunnel/pull/3719)
- [Bug] Fixed the problem that parsing the offset format failed when the startup mode was offset([3810](https://github.com/apache/seatunnel/pull/3810))
- [Feature] Kafka source supports data deserialization failure skipping([4364](https://github.com/apache/seatunnel/pull/4364))

