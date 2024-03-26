# Kafka

> Kafka 数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

> 默认情况下，我们将使用2pc来保证消息只发送一次到kafka。

## 描述

将 Rows 内容发送到 Kafka topic

## 支持的数据源信息

为了使用 Kafka 连接器，需要以下依赖项
可以通过 install-plugin.sh 或从 Maven 中央存储库下载

|  数据源  | 支持版本 |                                                 Maven                                                 |
|-------|------|-------------------------------------------------------------------------------------------------------|
| Kafka | 通用   | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-kafka) |

## 接收器选项

|          名称          |   类型   | 是否需要 | 默认值  |                                                                                                                                                                                                                    描述                                                                                                                                                                                                                     |
|----------------------|--------|------|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                | String | Yes  | -    | When the table is used as sink, the topic name is the topic to write data to.                                                                                                                                                                                                                                                                                                                                                             |
| bootstrap.servers    | String | Yes  | -    | Comma separated list of Kafka brokers.                                                                                                                                                                                                                                                                                                                                                                                                    |
| kafka.config         | Map    | 否    | -    | In addition to the above parameters that must be specified by the `Kafka producer` client, the user can also specify multiple non-mandatory parameters for the `producer` client, covering [all the producer parameters specified in the official Kafka document](https://kafka.apache.org/documentation.html#producerconfigs).                                                                                                           |
| semantics            | String | 否    | NON  | Semantics that can be chosen EXACTLY_ONCE/AT_LEAST_ONCE/NON, default NON.                                                                                                                                                                                                                                                                                                                                                                 |
| partition_key_fields | Array  | 否    | -    | Configure which fields are used as the key of the kafka message.                                                                                                                                                                                                                                                                                                                                                                          |
| partition            | Int    | 否    | -    | We can specify the partition, all messages will be sent to this partition.                                                                                                                                                                                                                                                                                                                                                                |
| assign_partitions    | Array  | 否    | -    | We can decide which partition to send based on the content of the message. The function of this parameter is to distribute information.                                                                                                                                                                                                                                                                                                   |
| transaction_prefix   | String | 否    | -    | If semantic is specified as EXACTLY_ONCE, the producer will write all messages in a Kafka transaction,kafka distinguishes different transactions by different transactionId. This parameter is prefix of  kafka  transactionId, make sure different job use different prefix.                                                                                                                                                             |
| format               | String | 否    | json | Data format. The default format is json. Optional text format, canal-json, debezium-json and avro.If you use json or text format. The default field separator is ", ". If you customize the delimiter, add the "field_delimiter" option.If you use canal format, please refer to [canal-json](../formats/canal-json.md) for details.If you use debezium format, please refer to [debezium-json](../formats/debezium-json.md) for details. |
| field_delimiter      | String | 否    | ,    | 自定义数据格式的字段分隔符                                                                                                                                                                                                                                                                                                                                                                                                                             |
| common-options       |        | 否    | -    | Sink插件常用参数，请参考 [Sink常用选项 ](../../transform-v2/common-options.md) 了解详情                                                                                                                                                                                                                                                                                                                                                                     |

## 参数解释

### Topic Formats

目前支持两种格式：

1. 填写主题名称

2. 使用上游数据中的字段值作为主题,格式是 `${your field name}`, 其中 topic 是上游数据的其中一列的值

   例如，上游数据如下：

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

如果 `${name}` 设置为主题。因此，第一行发送到 Jack 主题，第二行发送到 Mary 主题。

### 语义

在 EXACTLY_ONCE 中，生产者将在 Kafka 事务中写入所有消息，这些消息将在检查点上提交给 Kafka
在 AT_LEAST_ONCE 中，生产者将等待 Kafka 缓冲区中所有未完成的消息在检查点上被 Kafka 生产者确认
NON 不提供任何保证：如果 Kafka 代理出现问题，消息可能会丢失，并且消息可能会重复

### 分区关键字段

例如，如果您想使用上游数据中的字段值作为键，您可以将字段名称分配给该属性。

上游数据如下：

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

如果将 name 设置为 key，那么 name 列的哈希值将决定消息发送到哪个分区。如果将 name 设置为key，那么 name 列的哈希值将决定消息发送到哪个分区
如果没有设置分区键字段，则将发送空消息键
消息key的格式为json，如果设置 name 为key，例如 `{"name":"Jack"}`
所选字段必须是上游现有字段。所选字段必须是上游现有字段

### 分配分区

例如总共有5个分区，config中的assign_partitions字段如下：例如总共有5个分区，config中的assign_partitions字段如下：
assign_partitions = ["shoe", "clothing"]
那么包含`shoe`的消息将被发送到分区0，因为`shoe`在assign_partitions中被订阅为`0`，而包含`clothing`的消息将被发送到分区`1`。
对于其他消息，将使用哈希算法将它们分成剩余的分区。这个函数通过 `MessageContentPartitioner`
类实现了 `org.apache.kafka.clients. Producer.Partitioner` 接口。
如果我们需要自定义分区，我们也需要实现这个接口。

## 任务示例

### 简单:

> 本示例定义了一个 SeaTunnel 同步任务，通过 FakeSource 自动生成数据并将其发送到 Kafka Sink。
> FakeSource总共生成16行数据（row.num=16），每行有两个字段， name（string类型）和age（int类型）。
> 最终的目标主题是test_topic，也将是主题中的16行数据。并且如果
> 您尚未安装和部署SeaTunnel，则需要按照[安装SeaTunnel](../../start-v2/locally/deployment.md)中的说明
> 安装和部署SeaTunnel。然后按照[SeaTunnel Engine快速启动](../../start-v2/locally/quick-start-seatunnel-engine.md)中的说明运行
> 此作业

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
请确保 IAM 策略具有`kafka-cluster:Connect`
像这样：

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

