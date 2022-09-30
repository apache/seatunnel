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

## Example

###  Simple

```hocon
source {

    Kafka {
          topic = "seatunnel"
          bootstrap.servers = "localhost:9092"
          consumer.group = "seatunnel_group"
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