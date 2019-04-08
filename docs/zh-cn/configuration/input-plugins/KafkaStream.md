## Input plugin : KafkaStream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

从Kafka消费数据，支持的Kafka版本 >= 0.10.0.


### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| [topics](#topics-string) | string | yes | - | all streaming |
| [consumer.group.id](#consumergroupid-string) | string | yes | - | all streaming |
| [consumer.bootstrap.servers](#consumerbootstrapservers-string) | string | yes | - | all streaming |
| [consumer.*](#consumer-string) | string | no | - | all streaming |
| [table_name](#table_name-string) | string | no | - | Structured streaming |
| [offset.location](#offset.location-string) | string | no | - | Structured streaming |

##### topics [string]

Kafka topic名称。如果有多个topic，用","分割，例如: "tpc1,tpc2"。

##### consumer.group.id [string]

Kafka consumer group id，用于区分不同的消费组。structured streaming本不用设置group id，但是其group id 每次启动都是不一样的，为了便于监控，将这个参数加上。

##### consumer.bootstrap.servers [string]

Kafka集群地址，多个用","隔开

##### consumer.* [string]

除了以上必备的kafka consumer客户端必须指定的参数外，用户还可以指定多个consumer客户端非必须参数，覆盖了[kafka官方文档指定的所有consumer参数](http://kafka.apache.org/documentation.html#oldconsumerconfigs).

Spark Structured Streaming 中 Kafka Source 可选参数参考 [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#reading-data-from-kafka)

指定参数的方式是在原参数名称上加上前缀"consumer."，如指定`rebalance.max.retries`的方式是: `consumer.rebalance.max.retries = 100`。如果不指定这些非必须参数，它们将使用Kafka官方文档给出的默认值。

##### offset.location [string]

这个参数只有一个值，当你的checkpoint不可使用时，设置这个值为`broker`，将从broker获取offset进行消费。此参数仅在checkpoint不可使用时设置，否则可能发生不可预测的结果

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
### Notes
* 在structuredStreaming模式下，如果kafka里的数据是json格式，可以指定json的schema，input将按照指定的schema进行解析，如果你需要流关联功能，还需要指定`table_name`这个参数以便在sql插件中使用
如下
```
kafkaStream {
    topics = "waterdrop"
    consumer.bootstrap.servers = "localhost:9092"
    consumer.group.id = "waterdrop_group"
    consumer.rebalance.max.retries = 100
    consumer.failOnDataLoss = false
    schema = "{\"name\":\"string\",\"age\":\"integer\",\"addrs\":{\"country\":\"string\",\"city\":\"string\"}}"
}
```