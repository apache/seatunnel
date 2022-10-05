# Kafka

> Kafka sink connector

## Description

Write Rows to a Kafka topic.

:::tip

Engine Supported and plugin name

* [x] Spark: Kafka
* [x] Flink: Kafka

:::

## Options

| name                       | type   | required | default value |
| -------------------------- | ------ | -------- | ------------- |
| producer.bootstrap.servers | string | yes      | -             |
| topic                      | string | yes      | -             |
| producer.*                 | string | no       | -             |
| semantic                   | string | no       | -             |
| common-options             | string | no       | -             |

### producer.bootstrap.servers [string]

Kafka Brokers List

### topic [string]

Kafka Topic

### producer [string]

In addition to the above parameters that must be specified by the `Kafka producer` client, the user can also specify multiple non-mandatory parameters for the `producer` client, covering [all the producer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#producerconfigs).

The way to specify the parameter is to add the prefix `producer.` to the original parameter name. For example, the way to specify `request.timeout.ms` is: `producer.request.timeout.ms = 60000` . If these non-essential parameters are not specified, they will use the default values given in the official Kafka documentation.

### semantic [string]
Semantics that can be chosen. exactly_once/at_least_once/none, default is at_least_once

In exactly_once, flink producer will write all messages in a Kafka transaction that will be committed to Kafka on a checkpoint.

In at_least_once, flink producer will wait for all outstanding messages in the Kafka buffers to be acknowledged by the Kafka producer on a checkpoint.

NONE does not provide any guarantees: messages may be lost in case of issues on the Kafka broker and messages may be duplicated in case of a Flink failure.

please refer to [Flink Kafka Fault Tolerance](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/#fault-tolerance)

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](common-options.md) for details

## Examples

```bash
kafka {
    topics = "seatunnel"
    producer.bootstrap.servers = "localhost:9092"
}
```
