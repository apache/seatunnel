## Input plugin : Kafka

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

Read events from one or more kafka topics. Supporting Kafka >= 0.10.0


### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| [topics](#topics-string) | string | yes | - | all streaming |
| [consumer.group.id](#consumergroupid-string) | string | yes | - | spark streaming |
| [consumer.bootstrap.servers](#consumerbootstrapservers-string) | string | yes | - | all streaming |
| [consumer.*](#consumer-string) | string | no | - | all streaming |

##### topics [string]

Kafka topic. Multiple topics separated by commas. For example, "tpc1,tpc2".

##### consumer.group.id [string]

Kafka consumer group id, a unique string that identifies the consumer group this consumer belongs to. Only works on Spark Streaming application.

##### consumer.bootstrap.servers [string]

A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.This string should be in the form `host1:port1,host2:port2,.... `

##### consumer [string]

In addition to the above parameters that must be specified for the consumer client, you can also specify multiple kafka's consumer parameters described in [consumerconfigs](http://kafka.apache.org/10/documentation.html#consumerconfigs).

The Spark Structured Streaming optional configurations refer to [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#reading-data-from-kafka)

The way to specify parameters is to use the prefix "consumer" before the parameter. For example, `rebalance.max.retries` is specified as: `consumer.rebalance.max.retries = 100`.If you do not specify these parameters, it will be set the default values according to Kafka documentation


### Examples

* Spark Streaming

```
kafkaStream {
    topics = "waterdrop"
    consumer.bootstrap.servers = "localhost:9092"
    consumer.group.id = "waterdrop_group"
    consumer.rebalance.max.retries = 100
}
```

* Spark Structured Streaming

```
kafkaStream {
    topics = "waterdrop"
    consumer.bootstrap.servers = "localhost:9092"
    consumer.group.id = "waterdrop_group"
    consumer.rebalance.max.retries = 100
    consumer.failOnDataLoss = false
}
```