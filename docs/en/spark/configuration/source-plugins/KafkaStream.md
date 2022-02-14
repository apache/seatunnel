# KafkaStream

> Source plugin : KafkaStream [Spark]

## Description

To consumer data from `Kafka` , the supported `Kafka version >= 0.10.0` .

## Options

| name                       | type   | required | default value |
| -------------------------- | ------ | -------- | ------------- |
| topics                     | string | yes      | -             |
| consumer.group.id          | string | yes      | -             |
| consumer.bootstrap.servers | string | yes      | -             |
| consumer.*                 | string | no       | -             |
| common-options             | string | yes      | -             |

### topics [string]

`Kafka topic` name. If there are multiple `topics`, use `,` to split, for example: `"tpc1,tpc2"`

### consumer.group.id [string]

`Kafka consumer group id` , used to distinguish different consumer groups

### consumer.bootstrap.servers [string]

`Kafka` cluster address, separated by `,`

### consumer.* [string]

In addition to the above necessary parameters that must be specified by the `Kafka consumer` client, users can also specify multiple `consumer` client non-mandatory parameters, covering [all consumer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#oldconsumerconfigs) .

The way to specify parameters is to add the prefix `consumer.` to the original parameter name. For example, the way to specify `auto.offset.reset` is: `consumer.auto.offset.reset = latest` . If these non-essential parameters are not specified, they will use the default values given in the official Kafka documentation.

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```bash
kafkaStream {
    topics = "seatunnel"
    consumer.bootstrap.servers = "localhost:9092"
    consumer.group.id = "seatunnel_group"
}
```
