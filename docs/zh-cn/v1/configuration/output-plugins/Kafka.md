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
| [serializer](#serializer-string) | string | no | json | all streaming |
| [streaming_output_mode](#streaming_output_mode-string) | string | no | append | structured streaming |
| [checkpointLocation](#checkpointLocation-string) | string | no | - | structured streaming |
| [common-options](#common-options-string)| string | no | all streaming |


##### producer.bootstrap.servers [string]

Kafka Brokers List

##### topic [string]

Kafka Topic

##### producer [string]

除了以上必备的kafka producer客户端必须指定的参数外，用户还可以指定多个producer客户端非必须参数，覆盖了[kafka官方文档指定的所有producer参数](http://kafka.apache.org/documentation.html#producerconfigs).

指定参数的方式是在原参数名称上加上前缀"producer."，如指定`request.timeout.ms`的方式是: `producer.request.timeout.ms = 60000`。如果不指定这些非必须参数，它们将使用Kafka官方文档给出的默认值。


######  Notes

在作为structured streaming 的output的时候，你可以添加一些额外的参数，来达到相应的效果

##### checkpointLocation [string]

你可以指定是否启用checkpoint，通过配置**checkpointLocation**这个参数

##### streaming_output_mode [string]

你可以指定输出模式，complete|append|update三种，详见Spark文档http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes

##### serializer [string]

序列化方法，当前支持json和text，如果选择text方式，需保证数据结构中仅有一个字段。

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)


### Examples

> spark streaming or batch

```
kafka {
    topic = "waterdrop"
    producer.bootstrap.servers = "localhost:9092"
}
```
> structured streaming

```
kafka {
    topic = "waterdrop"
    producer.bootstrap.servers = "localhost:9092"
    streaming_output_mode = "update"
    checkpointLocation = "/your/path"
}
```