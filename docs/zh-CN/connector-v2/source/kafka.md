# Apache Kafka

> Apache Kafka 源连接器

## 描述

Apache Kafka 的源连接器。

## 主要特性

- [x] [批](../../concept/connector-v2-features.md)
- [x] [流](../../concept/connector-v2-features.md)
- [x] [精准一次](../../concept/connector-v2-features.md)
- [x] [模式投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义切分](../../concept/connector-v2-features.md)

##  选项

| 名字 | 类型 | 是否必须 | 默认值 |
| --- | --- | --- | --- |
| topic | String | Yes | - |
| pattern | Boolean | No | - |
| bootstrap.servers | String | Yes | - |
| consumer.group | String | No | SeaTunnel-Consumer-Group |
| commit_on_checkpoint | Boolean | No | - |
| schema | Config | No | content |
| format | String | No | json |
| result_table_name | String | No | - |


### topic [String]

Kafka topic 名称，如果有多个 topic，使用`,`来分割，例如:`tpc1,tpc2`，如果Pattern设置为`true`，
支持常规匹配主题，例如: `tpc.*`;

### pattern [Boolean]

是否启用常规匹配主题，使用java模式匹配 topic，设置为`true`启动常规匹配主题;

### bootstrap.servers [String]

kafka 集群的服务器地址，例如 : `hadoop101:9092,hadoop102:9092,hadoop103:9092`;

### consumer.group [String]

Kafka 消费者组，默认值是 `SeaTunnel-Consumer-Group`;

### commit_on_checkpoint [Boolean]

设置为`true`，消费者的偏移量将在后台定期提交;

### schema [Config]

用户定义的数据类型，参见文章:Schema;

### format [String]

数据格式，缺省情况下，读取`json`类型的数据。其他设置将被视为字符串，例如`json`;

### kafka. [String]

用于设置 Kafka 的配置，例如:`Kafka .max.poll.records = 500`，可以配置多个，依次添加到消费者的配置中;
详情请参见KafkaConsumer的配置;

### result_table_name [String]

读取后转换并在转换后的SQL查询中使用的表名;

## 比如

```kafka {
source {
  Kafka {
    result_table_name = "kafka_name"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
    format = json
    topic = "topic_1,topic_2,topic_3"
    bootstrap.server = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    kafka.max.poll.records = 500
    kafka.client.id = client_1
  }
}
```