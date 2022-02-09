# Sink plugin : Kafka [Flink]

## Description

Write data to Kafka

## Options

| name                       | type   | required | default value |
| -------------------------- | ------ | -------- | ------------- |
| producer.bootstrap.servers | string | yes      | -             |
| topic                      | string | yes      | -             |
| producer.*                 | string | no       | -             |
| common-options             | string | no       | -             |

### producer.bootstrap.servers [string]

Kafka Brokers List

### topic [string]

Kafka Topic

### producer [string]

In addition to the above mandatory parameters that must be specified by the `Kafka producer` client, the user can also specify multiple non-mandatory parameters for the `producer` client, covering [all the producer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#producerconfigs).

The way to specify the parameter is to add the prefix `producer.` to the original parameter name. For example, the way to specify `request.timeout.ms` is: `producer.request.timeout.ms = 60000` . If these non-essential parameters are not specified, they will use the default values given in the official Kafka documentation.

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Examples

```bash
   KafkaTable {
     producer.bootstrap.servers = "127.0.0.1:9092"
     topics = test_sink
   }
```
