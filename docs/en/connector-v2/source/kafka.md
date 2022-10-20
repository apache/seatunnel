# Kafka

> Kafka source connector

## Description

Source connector for Apache Kafka.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                 | type    | required | default value            |
| -------------------- | ------- | -------- | ------------------------ |
| topic                | String  | yes      | -                        |
| bootstrap.servers    | String  | yes      | -                        |
| pattern              | Boolean | no       | false                    |
| consumer.group       | String  | no       | SeaTunnel-Consumer-Group |
| commit_on_checkpoint | Boolean | no       | true                     |
| kafka.*              | String  | no       | -                        |
| common-options       |         | no       | -                        |
| schema               |         | no       | -                        |
| format               | String  | no       | json                     |

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

### kafka.* [string]

In addition to the above necessary parameters that must be specified by the `Kafka consumer` client, users can also specify multiple `consumer` client non-mandatory parameters, covering [all consumer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#consumerconfigs).

The way to specify parameters is to add the prefix `kafka.` to the original parameter name. For example, the way to specify `auto.offset.reset` is: `kafka.auto.offset.reset = latest` . If these non-essential parameters are not specified, they will use the default values given in the official Kafka documentation.

### common-options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

### schema
The structure of the data, including field names and field types.

## format
Data format. The default format is json. Optional text format. The default field separator is ", ".
If you customize the delimiter, add the "field_delimiter" option.

## Example

###  Simple

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
    field_delimiter = "#“
    topic = "topic_1,topic_2,topic_3"
    bootstrap.server = "localhost:9092"
    kafka.max.poll.records = 500
    kafka.client.id = client_1
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

## Changelog

### 2.3.0-beta 2022-10-20

- Add Kafka Source Connector
