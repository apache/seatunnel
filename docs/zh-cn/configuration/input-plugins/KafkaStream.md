## Input plugin : KafkaStream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

Kafka Input实现Kafka的Old Consumer客户端, 从Kafka消费数据。支持的Kafka版本 >= 0.8.2.X. 


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [topics](#topics-string) | string | yes | - |
| [consumer.group.id](#consumergroupid-string) | string | yes | - |
| [consumer.bootstrap.servers](#consumerbootstrapservers-string) | string | yes | - |
| [consumer.*](#consumer-string) | string | no | - |

##### topics [string]

Kafka topic名称。如果有多个topic，用","分割，例如: "tpc1,tpc2"。

##### consumer.group.id [string]

Kafka consumer group id，用于区分不同的消费组。

##### consumer.bootstrap.servers [string]

Kafka集群地址，多个用","隔开

##### consumer.* [string]

除了以上必备的kafka consumer客户端必须指定的参数外，用户还可以指定多个consumer客户端非必须参数，覆盖了[kafka官方文档指定的所有consumer参数](http://kafka.apache.org/documentation.html#oldconsumerconfigs).

指定参数的方式是在原参数名称上加上前缀"consumer."，如指定`rebalance.max.retries`的方式是: `consumer.rebalance.max.retries = 100`。如果不指定这些非必须参数，它们将使用Kafka官方文档给出的默认值。

### Examples

```
kafkaStream {
    topics = "waterdrop"
    consumer.bootstrap.servers = "localhost:9092"
    consumer.group.id = "waterdrop_group"
    consumer.rebalance.max.retries = 100
}
```