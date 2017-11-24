## Input plugin : Socket

* Author: garyelephant
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Kafka作为数据源

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [consumer.group.id](#consumergroupid-string) | string | yes | - |
| [consumer.zookeeper.connect](#consumerzookeeperconnect-string) | string | yes | - |
| [topic](#topic-string) | string | yes | - |

##### consumer.group.id [string]

Kafka consumer group id

##### consumer.zookeeper.connect [string]

Kafka zookeeper broker

##### topic [string]

Kafka Topic
