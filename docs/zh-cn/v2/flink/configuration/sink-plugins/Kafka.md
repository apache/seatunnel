## Sink plugin : Kafka [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
写入数据到Kafka

### Options

| name | type | required | default value | 
| --- | --- | --- | --- | 
| [producer.bootstrap.servers](#producerbootstrapservers-string) | string | yes | - | 
| [topic](#topic-string) | string | yes | - | 
| [producer.*](#producer-string) | string | no | - | 
| [common-options](#common-options-string)| string | no | - |


##### producer.bootstrap.servers [string]

Kafka Brokers List

##### topic [string]

Kafka Topic

##### producer [string]

除了以上必备的kafka producer客户端必须指定的参数外，用户还可以指定多个producer客户端非必须参数，覆盖了[kafka官方文档指定的所有producer参数](http://kafka.apache.org/documentation.html#producerconfigs).

指定参数的方式是在原参数名称上加上前缀"producer."，如指定`request.timeout.ms`的方式是: `producer.request.timeout.ms = 60000`。如果不指定这些非必须参数，它们将使用Kafka官方文档给出的默认值。

##### common options [string]

`Sink` 插件通用参数，详情参照 [Sink Plugin](/zh-cn/v2/flink/configuration/sink-plugins/)

### Examples

```
   KafkaTable {
     producer.bootstrap.servers = "127.0.0.1:9092"
     topics = test_sink
   }
```