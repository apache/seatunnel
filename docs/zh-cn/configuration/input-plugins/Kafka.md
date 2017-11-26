## Input plugin : Kafka

* Author: garyelephant
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Kafka作为数据源，Kafka支持的[参数](http://kafka.apache.org/documentation.html#consumerconfigs)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [topic](#topic-string) | string | yes | - |
| [consumer.group.id](#consumergroupid-string) | string | yes | - |
| [consumer.zookeeper.connect](#consumerzookeeperconnect-string) | string | yes | - |
| [consumer.bootstrap.servers](#consumerbootstrapservers-string) | string | yes | - |

##### topic [string]

Kafka数据集，多个数据集用**,**隔开

##### consumer.group.id [string]

Kafka消费组辨识号

##### consumer.zookeeper.connect [string]

Kafka集群依赖的Zookeeper地址

##### consumer.bootstrap.servers [string]

Kafka集群地址，多个用逗号隔开

### Examples

```
kafka {
    topics = "waterdrop"
    consumer.bootstrap.servers = "localhost:9092"
    consumer.zookeeper.connect = "localhost:2181"
    consumer.group.id = "waterdrop_streaming"
}
```