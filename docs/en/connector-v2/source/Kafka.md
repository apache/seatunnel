# Apache Kafka

> Apache Kafka source connector

## Description

Source connector for Apache Kafka.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

##  Options

| name | type | required | default value |
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

Kafka topic name, If there are multiple topics, use , to split, for example: "tpc1,tpc2", If Pattern is set to True, 
Support regular matching topic, for example: `tpc.*`;

### pattern [Boolean]

Whether to enable the regular matching topic, use java pattern match topic, Set to `true` to start the regular matching topic;

### bootstrap.servers [String]

The server address of kafka cluster, for example: `hadoop101:9092,hadoop102:9092,hadoop103:9092`;

### consumer.group [String]

Kafka consumer group. The default value is `SeaTunnel-Consumer-Group`;

### commit_on_checkpoint [Boolean]

If `true` the consumer's offset will be periodically committed in the background;

### schema [Config]

User - defined data type, refer to the article: Schema ;

### format [String]

Data format, By default, data of the JSON type is read. Other Settings will be treated as strings, for example `json`;

### kafka. [String]

Used to set up Kafka's configuration, for example: `kafka.max.poll.records = 500`, You can configure multiple, Will be added to the consumer's configuration;
For details, see Configuration of KafkaConsumer;

### result_table_name [String]

The table name that is converted after reading and used in the transformed SQL query;


## Example

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