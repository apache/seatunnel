## Output plugin : Kafka

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Write Rows to a Kafka topic.

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

In addition to the above parameters that must be specified for the producer client, you can also specify multiple kafka's producer parameters described in [producerconfigs](http://kafka.apache.org/10/documentation.html#producerconfigs)

The way to specify parameters is to use the prefix "producer" before the parameter. For example, `request.timeout.ms` is specified as: `producer.request.timeout.ms = 60000`.If you do not specify these parameters, it will be set the default values according to Kafka documentation


### Examples

```
kafka {
    topic = "waterdrop"
    producer.bootstrap.servers = "localhost:9092"
}
```