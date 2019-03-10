## Output plugin : Kafka

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出数据到Kafka

### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| [producer.bootstrap.servers](#producerbootstrapservers-string) | string | yes | - | all streaming |
| [topic](#topic-string) | string | yes | - | all streaming |
| [producer.*](#producer-string) | string | no | - | all streaming |

##### producer.bootstrap.servers [string]

Kafka Brokers List

##### topic [string]

Kafka Topic

##### producer [string]

除了以上必备的kafka producer客户端必须指定的参数外，用户还可以指定多个producer客户端非必须参数，覆盖了[kafka官方文档指定的所有producer参数](http://kafka.apache.org/documentation.html#producerconfigs).

指定参数的方式是在原参数名称上加上前缀"producer."，如指定`request.timeout.ms`的方式是: `producer.request.timeout.ms = 60000`。如果不指定这些非必须参数，它们将使用Kafka官方文档给出的默认值。

### Examples

```
kafka {
    topic = "waterdrop"
    producer.bootstrap.servers = "localhost:9092"
}
```
