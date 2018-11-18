## Input plugin : Kafka

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Read events from one or more kafka topics.

This plugin uses Kafka Old Consumer. Supporting Kafka >= 0.8.2.X


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [topics](#topics-string) | string | yes | - |
| [consumer.group.id](#consumergroupid-string) | string | yes | - |
| [consumer.bootstrap.servers](#consumerbootstrapservers-string) | string | yes | - |
| [consumer.*](#consumer-string) | string | no | - |

##### topics [string]

Kafka topic. Multiple topics separated by commas. For example, "tpc1,tpc2".

##### consumer.group.id [string]

Kafka consumer group id, a unique string that identifies the consumer group this consumer belongs to.

##### consumer.bootstrap.servers [string]

A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.This string should be in the form `host1:port1,host2:port2,.... `

##### consumer [string]

In addition to the above parameters that must be specified for the consumer client, you can also specify multiple kafka's consumer parameters described in [consumerconfigs](http://kafka.apache.org/10/documentation.html#consumerconfigs)

The way to specify parameters is to use the prefix "consumer" before the parameter. For example, `rebalance.max.retries` is specified as: `consumer.rebalance.max.retries = 100`.If you do not specify these parameters, it will be set the default values according to Kafka documentation


### Examples

```
kafka {
    topics = "waterdrop"
    consumer.bootstrap.servers = "localhost:9092"
    consumer.group.id = "waterdrop_group"
    consumer.rebalance.max.retries = 100
}
```