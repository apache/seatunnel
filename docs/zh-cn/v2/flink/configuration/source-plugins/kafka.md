## Source plugin : Kafka [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
从Kafka消费数据，支持的Kafka版本 >= 0.10.0.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [topics](#topics-string) | string | yes | - |
| [consumer.group.id](#consumergroupid-string) | string | yes | - |
| [consumer.bootstrap.servers](#consumerbootstrapservers-string) | string | yes | - |
| [schema](#schema-string) | string | yes | - | 
| [format](#format-string) | string | yes | - | 
| [format.*](#format.*-string) | string | no | - | 
| [consumer.*](#consumer-string) | string | no | - |
| [rowtime.field](#rowtime.field-string) | string | no | - | 
| [watermark](#watermark-string) | long | no | - | 
| [offset.reset](#offset.reset-string) | string | no | - | 
| [common-options](#common-options-string)| string | no | - |

##### topics [string]

Kafka topic名称。如果有多个topic，用","分割，例如: "tpc1,tpc2"。

##### consumer.group.id [string]

Kafka consumer group id，用于区分不同的消费组。

##### consumer.bootstrap.servers [string]

Kafka集群地址，多个用","隔开

##### format [string]
目前支持两种格式
- json
- csv

##### format.* [string]
csv格式通过这个参数来设置分隔符等。例如设置列分隔符为\t，`format.field-delimiter=\\t`

##### schema [string]
- csv
   - csv的schema是一个jsonArray的字符串，如`"[{\"field\":\"name\",\"type\":\"string\"},{\"field\":\"age\",\"type\":\"int\"}]"`。
- json
   - json的schema参数是提供一个原数据的json字符串，可以自动生成schema，但是需要提供内容最全的原数据，否则会有字段丢失。


##### consumer.* [string]

除了以上必备的kafka consumer客户端必须指定的参数外，用户还可以指定多个consumer客户端非必须参数，覆盖了[kafka官方文档指定的所有consumer参数](http://kafka.apache.org/documentation.html#oldconsumerconfigs).

指定参数的方式是在原参数名称上加上前缀"consumer."，如指定`ssl.key.password`的方式是: `consumer.ssl.key.password= xxxx`。如果不指定这些非必须参数，它们将使用Kafka官方文档给出的默认值。

##### rowtime.field [string]
设置生成watermark的字段

##### watermark [long]
设置生成watermark的允许延迟

##### offset.reset [string]
消费者的起始offset，只对新消费者有效。有以下三种模式
- latest 
  - 从最新的offset开始消费
- earliest 
  - 从最早的offset开始消费
- specific 
  - 从指定的offset开始消费，此时要指定各个分区的起始offset。设置方式通过`offset.reset.specific="{0:111,1:123}"`

##### common options [string]

`Source` 插件通用参数，详情参照 [Source Plugin](/zh-cn/v2/flink/configuration/source-plugins/)

### Examples

```
  KafkaTableStream {
    consumer.bootstrap.servers = "127.0.0.1:9092"
    consumer.group.id = "waterdrop5"
    topics = test
    result_table_name = test
    format = csv
    schema = "[{\"field\":\"name\",\"type\":\"string\"},{\"field\":\"age\",\"type\":\"int\"}]"
    format.field-delimiter = ";"
    format.allow-comments = "true"
    format.ignore-parse-errors = "true"
  }
```
