# Kafka

> Kafka 数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

> 默认情况下，我们将使用 2pc 来保证消息只发送一次到kafka

## 描述

将 Rows 内容发送到 Kafka topic

## 支持的数据源信息

为了使用 Kafka 连接器，需要以下依赖项
可以通过 install-plugin.sh 或从 Maven 中央存储库下载

| 数据源   | 支持版本 | Maven                                                                         |
|-------|------|-------------------------------------------------------------------------------|
| Kafka | 通用   | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-kafka) |

## 接收器选项

|          名称          |   类型   | 是否需要 | 默认值  | 描述                                                                                                                                                                                                                                                        |
|----------------------|--------|------|------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                | String | 是    | -    | 当表用作接收器时，topic 名称是要写入数据的 topic                                                                                                                                                                                                                            |
| bootstrap.servers    | String | 是    | -    | Kafka brokers 使用逗号分隔                                                                                                                                                                                                                                      |
| kafka.config         | Map    | 否    | -    | 除了上述 Kafka Producer 客户端必须指定的参数外，用户还可以为 Producer 客户端指定多个非强制参数，涵盖 [Kafka官方文档中指定的所有生产者参数](https://kafka.apache.org/documentation.html#producerconfigs)                                                                                                       |
| semantics            | String | 否    | NON  | 可以选择的语义是 EXACTLY_ONCE/AT_LEAST_ONCE/NON，默认 NON。                                                                                                                                                                                                           |
| partition_key_fields | Array  | 否    | -    | 配置字段用作 kafka 消息的key                                                                                                                                                                                                                                       |
| partition            | Int    | 否    | -    | 可以指定分区，所有消息都会发送到此分区                                                                                                                                                                                                                                       |
| assign_partitions    | Array  | 否    | -    | 可以根据消息的内容决定发送哪个分区,该参数的作用是分发信息                                                                                                                                                                                                                             |
| transaction_prefix   | String | 否    | -    | 如果语义指定为EXACTLY_ONCE，生产者将把所有消息写入一个 Kafka 事务中，kafka 通过不同的 transactionId 来区分不同的事务。该参数是kafka transactionId的前缀，确保不同的作业使用不同的前缀                                                                                                                                  |
| format               | String | 否    | json | 数据格式。默认格式是json。可选文本格式，canal-json、debezium-json 、 avro 和 protobuf。如果使用 json 或文本格式。默认字段分隔符是`,`。如果自定义分隔符，请添加`field_delimiter`选项。如果使用canal格式，请参考[canal-json](../formats/canal-json.md)。如果使用debezium格式，请参阅 [debezium-json](../formats/debezium-json.md) 了解详细信息 |
| field_delimiter      | String | 否    | ,    | 自定义数据格式的字段分隔符                                                                                                                                                                                                                                             |
| common-options       |        | 否    | -    | Sink插件常用参数，请参考 [Sink常用选项 ](../sink-common-options.md) 了解详情                                                                                                                                                                                                |
|protobuf_message_name|String|否|-| format配置为protobuf时生效，取Message名称                                                                                                                                                                                                                           |
|protobuf_schema|String|否|-| format配置为protobuf时生效取Schema名称                                                                                                                                                                                                                                      |

## 参数解释

### Topic 格式

目前支持两种格式：

1. 填写topic名称

2. 使用上游数据中的字段值作为 topic ,格式是 `${your field name}`, 其中 topic 是上游数据的其中一列的值

   例如，上游数据如下：

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

如果 `${name}` 设置为 topic。因此，第一行发送到 Jack topic，第二行发送到 Mary topic。

### 语义

在 EXACTLY_ONCE 中，生产者将在 Kafka 事务中写入所有消息，这些消息将在检查点上提交给 Kafka，该模式下能保证数据精确写入kafka一次，即使任务失败重试也不会出现数据重复和丢失
在 AT_LEAST_ONCE 中，生产者将等待 Kafka 缓冲区中所有未完成的消息在检查点上被 Kafka 生产者确认，该模式下能保证数据至少写入kafka一次，即使任务失败
NON 不提供任何保证：如果 Kafka 代理出现问题，消息可能会丢失，并且消息可能会重复，该模式下，任务失败重试可能会产生数据丢失或重复。

### 分区关键字段

例如，如果你想使用上游数据中的字段值作为键，可以将这些字段名指定给此属性

上游数据如下所示：

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

如果将 name 设置为 key，那么 name 列的哈希值将决定消息发送到哪个分区。
如果没有设置分区键字段，则将发送空消息键。
消息 key 的格式为 json，如果设置 name 为 key，例如 `{"name":"Jack"}`。
所选的字段必须是上游数据中已存在的字段。

### 分区分配

假设总有五个分区，配置中的 assign_partitions 字段设置为：
assign_partitions = ["shoe", "clothing"]
在这种情况下，包含 "shoe" 的消息将被发送到第零个分区，因为 "shoe" 在 assign_partitions 中被标记为零， 而包含 "clothing" 的消息将被发送到第一个分区。
对于其他的消息，我们将使用哈希算法将它们均匀地分配到剩余的分区中。
这个功能是通过 MessageContentPartitioner 类实现的，该类实现了 org.apache.kafka.clients.producer.Partitioner 接口。如果我们需要自定义分区，我们需要实现这个接口。

## 任务示例

### 简单:

> 此示例展示了如何定义一个 SeaTunnel 同步任务，该任务能够通过 FakeSource 自动产生数据并将其发送到 Kafka Sink。在这个例子中，FakeSource 会生成总共 16 行数据（`row.num=16`），每一行都包含两个字段，即 `name`（字符串类型）和 `age`（整型）。最终，这些数据将被发送到名为 test_topic 的 topic 中，因此该 topic 也将包含 16 行数据。
> 如果你还未安装和部署 SeaTunnel，你需要参照 [安装SeaTunnel](../../start-v2/locally/deployment.md) 的指南来进行安装和部署。完成安装和部署后，你可以按照 [快速开始使用 SeaTunnel 引擎](../../start-v2/locally/quick-start-seatunnel-engine.md) 的指南来运行任务。

```hocon
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    result_table_name = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  kafka {
      topic = "test_topic"
      bootstrap.servers = "localhost:9092"
      format = json
      kafka.request.timeout.ms = 60000
      semantics = EXACTLY_ONCE
      kafka.config = {
        acks = "all"
        request.timeout.ms = 60000
        buffer.memory = 33554432
      }
  }
}
```

### AWS MSK SASL/SCRAM

将以下 `${username}` 和 `${password}` 替换为 AWS MSK 中的配置值。

```hocon
sink {
  kafka {
      topic = "seatunnel"
      bootstrap.servers = "localhost:9092"
      format = json
      kafka.request.timeout.ms = 60000
      semantics = EXACTLY_ONCE
      kafka.config = {
         security.protocol=SASL_SSL
         sasl.mechanism=SCRAM-SHA-512
         sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required \nusername=${username}\npassword=${password};"
      }
  }
}
```

### AWS MSK IAM

从 https://github.com/aws/aws-msk-iam-auth/releases 下载 `aws-msk-iam-auth-1.1.5.jar`
并将其放入 `$SEATUNNEL_HOME/plugin/kafka/lib` 中目录。
请确保 IAM 策略具有 `kafka-cluster:Connect`
如下配置：

```hocon
"Effect": "Allow",
"Action": [
    "kafka-cluster:Connect",
    "kafka-cluster:AlterCluster",
    "kafka-cluster:DescribeCluster"
],
```

接收器配置

```hocon
sink {
  kafka {
      topic = "seatunnel"
      bootstrap.servers = "localhost:9092"
      format = json
      kafka.request.timeout.ms = 60000
      semantics = EXACTLY_ONCE
      kafka.config = {
         security.protocol=SASL_SSL
         sasl.mechanism=AWS_MSK_IAM
         sasl.jaas.config="software.amazon.msk.auth.iam.IAMLoginModule required;"
         sasl.client.callback.handler.class="software.amazon.msk.auth.iam.IAMClientCallbackHandler"
      }
  }
}
```

### Protobuf配置

`format` 设置为 `protobuf`，配置`protobuf`数据结构，`protobuf_message_name`和`protobuf_schema`参数

使用样例：

```hocon
sink {
  kafka {
      topic = "test_protobuf_topic_fake_source"
      bootstrap.servers = "kafkaCluster:9092"
      format = protobuf
      kafka.request.timeout.ms = 60000
      kafka.config = {
        acks = "all"
        request.timeout.ms = 60000
        buffer.memory = 33554432
      }
      protobuf_message_name = Person
      protobuf_schema = """
              syntax = "proto3";

              package org.apache.seatunnel.format.protobuf;

              option java_outer_classname = "ProtobufE2E";

              message Person {
                int32 c_int32 = 1;
                int64 c_int64 = 2;
                float c_float = 3;
                double c_double = 4;
                bool c_bool = 5;
                string c_string = 6;
                bytes c_bytes = 7;

                message Address {
                  string street = 1;
                  string city = 2;
                  string state = 3;
                  string zip = 4;
                }

                Address address = 8;

                map<string, float> attributes = 9;

                repeated string phone_numbers = 10;
              }
              """
  }
}
```

