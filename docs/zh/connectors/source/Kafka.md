import ChangeLog from '../changelog/connector-kafka.md';

# Kafka

> Kafka 源连接器

## 支持以下引擎

> Spark<br/>  
> Flink<br/>  
> Seatunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义拆分](../../introduction/concepts/connector-v2-features.md)

## 描述

用于 Apache Kafka 的源连接器。

## 支持的数据源信息

使用 Kafka 连接器需要以下依赖项。  
可以通过 install-plugin.sh 下载或从 Maven 中央仓库获取。

| 数据源   | 支持的版本 | Maven 下载链接                                                                    |
|-------|-------|-------------------------------------------------------------------------------|
| Kafka | 通用版本  | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-kafka) |

## 源选项

| 名称                                  | 类型                                  | 是否必填 | 默认值                          | 描述                                                                                                                                                                                                                                                                                                                             |
|-------------------------------------|-------------------------------------|------|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                               | String                              | 否    | -                            | 使用表作为数据源时要读取数据的主题名称。未使用 `tables_configs` 或 `table_list` 时需要配置。也支持通过逗号分隔多个主题，例如 `topic-1,topic-2`。                                                                                                                                                                                                                         |
| tables_configs                      | List                                | 否    | -                            | 推荐使用的多主题表配置。当不同 topic 需要不同 schema 或 format 时使用。`topic`、`tables_configs`、`table_list` 三者只能配置一个。                                                                                                                                                                                                                   |
| table_list                          | List                                | 否    | -                            | 旧版兼容的多主题表配置。`topic`、`tables_configs`、`table_list` 三者只能配置一个。                                                                                                                                                                                                                                                                  |
| bootstrap.servers                   | String                              | 是    | -                            | 逗号分隔的 Kafka brokers 列表。                                                                                                                                                                                                                                                                                                        |
| pattern                             | Boolean                             | 否    | false                        | 如果 `pattern` 设置为 `true`，则会使用指定的正则表达式匹配并订阅主题。                                                                                                                                                                                                                                                                                   |
| consumer.group                      | String                              | 否    | SeaTunnel-Consumer-Group     | `Kafka 消费者组 ID`，用于区分不同的消费者组。                                                                                                                                                                                                                                                                                                   |
| commit_on_checkpoint                | Boolean                             | 否    | true                         | 如果为 true，仅在 SeaTunnel checkpoint 完成后提交消费者偏移量，并禁用 Kafka 自动提交；如果为 false，则禁用 checkpoint 提交并启用 Kafka 自动提交。                                                                                                                                                                                                                                                               |
| poll.timeout                        | Long                                | 否    | 10000                        | kafka主动拉取时间间隔(毫秒)。                                                                                                                                                                                                                                                                                                             |
| kafka.config                        | Map                                 | 否    | -                            | 除了上述必要参数外，用户还可以指定多个非强制的消费者客户端参数，覆盖 [Kafka 官方文档](https://kafka.apache.org/documentation.html#consumerconfigs) 中指定的所有消费者参数。                                                                                                                                                                                                      |
| schema                              | Config                              | 否    | -                            | 数据结构，包括字段名称和字段类型。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                                                                                                                                                                                                                                                                    |
| format                              | String                              | 否    | json                         | 数据格式。默认格式为 json。可选格式包括 text, canal_json, debezium_json, ogg_json, maxwell_json, avro , protobuf和native。默认字段分隔符为 ", "。如果自定义分隔符，添加 "field_delimiter" 选项。如果使用 canal 格式，请参考 [canal-json](../formats/canal-json.md) 了解详细信息。如果使用 debezium 格式，请参考 [debezium-json](../formats/debezium-json.md)。一些Format的详细信息请参考 [formats](../formats) |
| avro_schema                         | String                              | 否    | -                            | 当 `format` 为 `avro` 时生效。用于提供二进制 Avro 消息的 writer schema，适用于消息的 record 名称、namespace 或 union 结构与 SeaTunnel schema 不完全一致的场景。                                                                                                                                                                                                                                             |
| format_error_handle_way             | String                              | 否    | fail                         | 数据格式错误的处理方式。默认值为 fail，可选值为 fail 和 skip。当选择 fail 时，数据格式错误将阻塞并抛出异常。当选择 skip 时，数据格式错误将跳过此行数据。                                                                                                                                                                                                                                     |
| debezium_record_include_schema      | Boolean                             | 否    | true                         | 当 `format` 为 `debezium_json` 时生效，用于说明 Debezium 记录中是否携带 schema 信息。                                                                                                                                                                                                                                          |
| debezium_record_table_filter        | Config                              | 否    | -                            | 用于过滤 debezium 格式的数据，仅当格式设置为 `debezium_json` 时使用。请参阅下面的 `debezium_record_table_filter`                                                                                                                                                                                                                                          |
| field_delimiter                     | String                              | 否    | ,                            | 自定义数据格式的字段分隔符。                                                                                                                                                                                                                                                                                                                 |
| start_mode                          | StartMode[earliest],[group_offsets],[latest],[specific_offsets],[timestamp] | 否    | group_offsets                | 消费者的初始消费模式。                                                                                                                                                                                                                                                                                                                    |
| start_mode.offsets                  | Config                              | 否    | -                            | 用于 specific_offsets 消费模式的偏移量。                                                                                                                                                                                                                                                                                                  |
| start_mode.timestamp                | Long                                | 否    | -                            | 用于 "timestamp" 消费模式的时间。                                                                                                                                                                                                                                                                                                        |
| start_mode.end_timestamp             | Long                                | 否    | -                            | 用于 "timestamp" 消费模式的结束时间，只支持批模式。                                                                                                                                                                                                                                                                                             |
| partition-discovery.interval-millis | Long                                | 否    | -1                           | 动态发现主题和分区的间隔时间。                                                                                                                                                                                                                                                                                                                |
| ignore_no_leader_partition          | Boolean                             | 否    | false                        | 是否忽略没有 leader 的分区。如果设置为 true，在分区发现过程中将跳过没有 leader 的分区。如果设置为 false（默认值），连接器将包含所有分区，无论 leader 状态如何。这在处理可能存在临时 leader 问题的 Kafka 集群时很有用。                                                                                                                                                                                  |
| common-options                      |                                     | 否    | -                            | 源插件的常见参数，详情请参考 [Source Common Options](../common-options/source-common-options.md)。                                                                                                                                                                                                                                                           |
| protobuf_message_name               | String                              | 否    | -                            | 当格式设置为 protobuf 时有效，指定消息名称。                                                                                                                                                                                                                                                                                                    |
| protobuf_schema                     | String                              | 否    | -                            | 当格式设置为 protobuf 时有效，指定 Schema 定义。                                                                                                                                                                                                                                                                                              |
| strip_schema_registry_header        | Boolean                             | 否    | false                        | 当格式设置为 protobuf 或 avro 时有效。protobuf 会在反序列化前去除 Confluent Schema Registry 头；avro 会去除固定的 5 字节头（magic byte 和 schema ID）。avro 启用此选项时必须同时配置 `avro_schema`，且不会查询 Schema Registry。 |
| reader_cache_queue_size             | Integer                             | 否    | 2                            | Fetcher 与 Reader 线程之间缓冲队列的容量。每个元素是一次 `consumer.poll()` 的整批结果，而非单条消息。详见 [reader_cache_queue_size](#reader_cache_queue_size)。 |
| is_native                           | Boolean                             | 否    | false                        | 支持保留record的源信息。                                                                                                                                                                                                                                                                                                                |
| kafka_headers_fields                | Array                               | 否    | -                            | 指定要从 Kafka 消息 header 中提取并映射为行字段的 header key 列表。每个 header 值以 STRING 类型追加到输出行的末尾（位于正常 schema 字段之后）。不支持 NATIVE 格式。                                                                                                                                                                                                               |

> 从 checkpoint 或 savepoint 恢复时，Kafka Source 会优先使用 checkpoint 中保存的 split offset。
> `start_mode` 和 consumer group offset 只在首次启动，或为尚未存在 checkpoint 状态的新发现分区初始化位点时生效。

:::tip

读取一个或多个 topic 时使用 `topic`。如果不同 topic 需要不同的 schema 或 format，请使用 `tables_configs`。`topic`、`tables_configs`、`table_list` 互斥，不能同时配置。

:::

### reader_cache_queue_size

连接器在 Fetcher 线程和 Reader 线程之间缓冲的 poll 结果批次的最大数量。

:::tip

队列中的每个元素是一次完整的 `consumer.poll()` 结果，最多包含 `max.poll.records`（默认 500）条消息。
当下游产生背压时，队列可能被填满，此时驻留在内存中的消息数上限为 `reader_cache_queue_size × max.poll.records`。

:::

:::caution

当消费的消息体较大时，过高的值会导致堆内存占用过高。如果观察到内存压力，请减小此值或降低 `kafka.max.poll.records`。

:::

### debezium_record_table_filter

我们可以使用 `debezium_record_table_filter` 来过滤 debezium 格式的数据。配置如下：

```hocon
debezium_record_table_filter {
  database_name = "test"
  schema_name = "public" // null 如果不存在
  table_name = "products"
}
```

只有 `test.public.products` 表的数据将被消费。

## 元数据支持

Kafka 源会在 `ConsumerRecord.timestamp` 大于等于 0 时，将其自动写入 SeaTunnel 行的 `EventTime` 元数据。可以借助 [Metadata 转换](../../transforms/metadata.md) 把这段时间戳暴露为普通字段，方便做分区或下游 SQL 处理。

```hocon
source {
  Kafka {
    plugin_output = "kafka_raw"
    topic = "seatunnel_topic"
    bootstrap.servers = "localhost:9092"
    format = json
  }
}

transform {
  Metadata {
    plugin_input = "kafka_raw"
    plugin_output = "kafka_with_meta"
    metadata_fields {
      EventTime = kafka_ts # ConsumerRecord.timestamp (ms)
    }
  }
  Sql {
    plugin_input = "kafka_with_meta"
    plugin_output = "kafka_enriched"
    query = "select *, FROM_UNIXTIME(kafka_ts/1000, 'yyyy-MM-dd', 'Asia/Shanghai') as pt from kafka_with_meta where kafka_ts >= 0"
  }
}
```

## 任务示例

### 读取 Kafka 消息 Header

使用 `kafka_headers_fields` 将指定的 Kafka 消息 header 提取为行字段。header 值以 STRING 类型追加在正常 schema 字段之后。

> 注意：不支持 `NATIVE` 格式，该格式已通过 `Map<String, String>` 字段暴露了所有 header。

```hocon
source {
  Kafka {
    topic = "my-topic"
    bootstrap.servers = "localhost:9092"
    kafka_headers_fields = ["correlation-id", "x-trace-id"]
    schema = {
      fields {
        user_id = "int"
        name = "string"
      }
    }
    format = json
  }
}
```

输出行将包含：`user_id`（int）、`name`（string）、`correlation-id`（string）、`x-trace-id`（string）。  
如果某条消息中不存在对应的 header key，则该字段值为 `null`。

此功能与 Kafka sink 连接器的 `kafka_headers_fields` 对应，支持 header 在 topic 间的全链路传递。

### 简单示例

> 此示例读取 Kafka 的 topic_1、topic_2 和 topic_3 的数据并将其打印到客户端。如果尚未安装和部署 SeaTunnel，请按照 [安装指南](../../getting-started/locally/deployment.md) 进行安装和部署。然后，按照 [快速开始](../../getting-started/locally/quick-start-seatunnel-engine.md) 运行此任务。

```hocon
# 定义运行环境
env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
  Kafka {
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
    format = text
    field_delimiter = "#"
    topic = "topic_1,topic_2,topic_3"
    bootstrap.servers = "localhost:9092"
    kafka.config = {
      client.id = client_1
      max.poll.records = 500
      auto.offset.reset = "earliest"
      enable.auto.commit = "false"
    }
  }  
}
sink {
  Console {}
}
```

### 正则表达式主题

```hocon
source {
    Kafka {
          topic = ".*seatunnel*."
          pattern = true
          bootstrap.servers = "localhost:9092"
          consumer.group = "seatunnel_group"
    }
}
```

### 动态发现分区

流任务运行期间，如果 Kafka topic 会新增分区，可以配置 `partition-discovery.interval-millis` 定时发现新分区。新分区没有 checkpoint 位点时，会按 `start_mode` 初始化消费位置。

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Kafka {
    topic = "seatunnel_topic"
    bootstrap.servers = "localhost:9092"
    consumer.group = "seatunnel_group"
    start_mode = latest
    partition-discovery.interval-millis = 5000
    format = json
  }
}
```

### AWS MSK SASL/SCRAM

将以下 `${username}` 和 `${password}` 替换为 AWS MSK 中的配置值。

```hocon
source {
    Kafka {
        topic = "seatunnel"
        bootstrap.servers = "xx.amazonaws.com.cn:9096,xxx.amazonaws.com.cn:9096,xxxx.amazonaws.com.cn:9096"
        consumer.group = "seatunnel_group"
        kafka.config = {
            security.protocol=SASL_SSL
            sasl.mechanism=SCRAM-SHA-512
            sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";"
        }
    }
}
```

### AWS MSK IAM

从 [此处](https://github.com/aws/aws-msk-iam-auth/releases) 下载 `aws-msk-iam-auth-1.1.5.jar` 并将其放在 `$SEATUNNEL_HOME/plugin/kafka/lib` 目录下。

确保 IAM 策略中包含 `"kafka-cluster:Connect"` 权限，如下所示：

```hocon
"Effect": "Allow",
"Action": [
    "kafka-cluster:Connect",
    "kafka-cluster:AlterCluster",
    "kafka-cluster:DescribeCluster"
],
```

源配置示例：

```hocon
source {
    Kafka {
        topic = "seatunnel"
        bootstrap.servers = "xx.amazonaws.com.cn:9098,xxx.amazonaws.com.cn:9098,xxxx.amazonaws.com.cn:9098"
        consumer.group = "seatunnel_group"
        kafka.config = {
            security.protocol=SASL_SSL
            sasl.mechanism=AWS_MSK_IAM
            sasl.jaas.config="software.amazon.msk.auth.iam.IAMLoginModule required;"
            sasl.client.callback.handler.class="software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        }
    }
}
```

### Kerberos 认证示例

请在启动 SeaTunnel 之前设置 JVM 参数 `java.security.krb5.conf` 或更新 `/etc/krb5.conf` 中的默认 `krb5.conf`。

源配置示例：

```hocon
source {
    Kafka {
        topic = "seatunnel"
        bootstrap.servers = "127.0.0.1:9092"
        consumer.group = "seatunnel_group"
        kafka.config = {
            security.protocol=SASL_PLAINTEXT
            sasl.kerberos.service.name=kafka
            sasl.mechanism=GSSAPI
            sasl.jaas.config="com.sun.security.auth.module.Krb5LoginModule required \n        useKeyTab=true \n        storeKey=true  \n        keyTab=\"/path/to/xxx.keytab\" \n        principal=\"user@xxx.com\";"
        }
    }
}
```

### 多 Kafka 源示例

> 根据不同的 Kafka 主题和格式解析数据，并基于 ID 执行 upsert 操作。

> 注意: Kafka是一个非结构化数据源，应该使用`tables_configs`，将来会删除`table_list`

```hocon

env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafka_e2e:9092"
    tables_configs = [
      {
        topic = "^test-ogg-sou.*"
        pattern = true
        consumer.group = "ogg_multi_group"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = ogg_json
      },
      {
        topic = "test-cdc_mds"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = canal_json
      }
    ]
  }
}

sink {
  Jdbc {
    driver = org.postgresql.Driver
    url = "jdbc:postgresql://postgresql:5432/test?loggerLevel=OFF"
    user = test
    password = test
    generate_sink_sql = true
    database = test
    table = public.sink
    primary_keys = ["id"]
  }
}
```

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  Kafka {
   

 bootstrap.servers = "kafka_e2e:9092"
    table_list = [
      {
        topic = "^test-ogg-sou.*"
        pattern = true
        consumer.group = "ogg_multi_group"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = ogg_json
      },
      {
        topic = "test-cdc_mds"
        start_mode = earliest
        schema = {
          fields {
            id = "int"
            name = "string"
            description = "string"
            weight = "string"
          }
        },
        format = canal_json
      }
    ]
  }
}

sink {
  Jdbc {
    driver = org.postgresql.Driver
    url = "jdbc:postgresql://postgresql:5432/test?loggerLevel=OFF"
    user = test
    password = test
    generate_sink_sql = true
    database = test
    table = public.sink
    primary_keys = ["id"]
  }
}
```

### Protobuf配置

`format` 设置为 `protobuf`，配置`protobuf`数据结构，`protobuf_message_name`和`protobuf_schema`参数

使用样例：

```hocon
source {
  Kafka {
    topic = "test_protobuf_topic_fake_source"
    format = protobuf
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
    bootstrap.servers = "kafkaCluster:9092"
    start_mode = "earliest"
    plugin_output = "kafka_table"
  }
}
```

### Protobuf with Schema Registry wire format

当消费使用 Confluent Schema Registry 编码的 Protobuf 消息时，您需要将 `strip_schema_registry_header` 设置为 `true`。连接器将自动检测并删除 Schema Registry 格式头部（magic byte、schema id 和 message indexes），然后再反序列化 Protobuf 消息。

使用样例：

```hocon
source {
  Kafka {
    topic = "test_protobuf_schema_registry_topic"
    format = protobuf
    strip_schema_registry_header = true
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
    bootstrap.servers = "kafkaCluster:9092"
    start_mode = "earliest"
    plugin_output = "kafka_table"
  }
}
```

**注意**：当启用 `strip_schema_registry_header` 时，连接器可以安全地处理 Schema Registry 编码的消息和纯 Protobuf 消息。如果未检测到 Schema Registry 头部，它将自动回退到标准 Protobuf 反序列化。
```

### 忽略无 Leader 分区

当处理可能存在临时 leader 问题的 Kafka 集群时，您可以配置连接器忽略没有 leader 的分区：

```hocon
source {
  Kafka {
    topic = "test_topic"
    bootstrap.servers = "localhost:9092"
    consumer.group = "test_group"
    ignore_no_leader_partition = true
    start_mode = "earliest"
  }
}
```

当 `ignore_no_leader_partition = true` 时，连接器将在分区发现过程中跳过任何没有 leader 的分区，允许作业继续处理其他健康的分区。

### format
如果需要保留Kafka原生的信息，可以参考如下配置。

配置示例:
```hocon
source {
  Kafka {
    topic = "test_topic_native_source"
    bootstrap.servers = "kafkaCluster:9092"
    start_mode = "earliest"
    format_error_handle_way = skip
    format = "NATIVE"
    value_converter_schema_enabled = false
    consumer.group = "native_group"
  }
}
```

返回数据格式如下:
```json
{
  "headers": {
    "header1": "header1",
    "header2": "header2"
  },
  "key": "dGVzdF9ieXRlc19kYXRh",
  "partition": 3,
  "timestamp": 1672531200000,
  "timestampType": "CREATE_TIME",
  "value": "dGVzdF9ieXRlc19kYXRh"
}
```
注意：key/value是byte[]类型。

### 配合动态分区发现与 EXACTLY_ONCE 下游的流式作业

常见的长时间运行模式：使用 Kafka 源读取数据并写入 Kafka sink，配合 checkpoint 与 `semantics = EXACTLY_ONCE` 实现端到端精确一次。开启 `partition-discovery.interval-millis` 后，新增分区会被自动发现，无需重启作业。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Kafka {
    topic = "orders"
    bootstrap.servers = "localhost:9092"
    consumer.group = "orders_consumer"
    start_mode = group_offsets
    commit_on_checkpoint = true
    partition-discovery.interval-millis = 30000
    format = json
    schema = {
      fields {
        order_id = bigint
        user_id = bigint
        amount = double
      }
    }
  }
}

sink {
  Kafka {
    topic = "orders_sink"
    bootstrap.servers = "localhost:9092"
    format = json
    semantics = EXACTLY_ONCE
    transaction_prefix = "orders_sink_job"
    partition_key_fields = ["order_id"]
  }
}
```

同样的写法也适用于 `format = debezium_json`，可以从 Kafka Connect sink 输出的 Debezium 变更事件中消费变更数据并转发到下游。

### Avro 反序列化

当 Avro 消息的 record 名称、namespace 或 union 结构与 SeaTunnel schema 不一致时，需要将 `format` 设置为 `avro` 并提供 `avro_schema`。如果不提供 `avro_schema`，连接器会从用户配置的 `schema` 块派生解码 schema，并同时将其作为 reader schema 和 writer schema 使用；如果生产端的 Avro 结构（record 名称、namespace、union 结构）与 SeaTunnel schema 不一致，请显式配置 `avro_schema`。当前实现中没有 Confluent Schema Registry 查询或按消息回退读取 schema 的机制。

```hocon
source {
  Kafka {
    topic = "users_avro"
    bootstrap.servers = "localhost:9092"
    format = avro
    avro_schema = """
      {
        "type": "record",
        "name": "User",
        "namespace": "com.example",
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "email", "type": ["null", "string"], "default": null}
        ]
      }
      """
    schema = {
      fields {
        id = bigint
        name = string
        email = string
      }
    }
  }
}
```

## 常见问题

### `start_mode` 各取值有什么区别？

| `start_mode` | 行为 |
|---|---|
| `earliest` | 从每个分区最早可用的 offset 开始消费 |
| `latest` | 只消费任务启动后新产生的消息 |
| `group_offsets` | 从消费组已提交的 offset 恢复消费 |
| `specific_offsets` | 从每个分区指定的 offset 开始消费 |
| `timestamp` | 从指定时间戳处或其后的第一条消息开始消费 |

任务中断后重启时使用 `group_offsets` 恢复；需要从头重放全量数据时使用 `earliest`。

### 如何按 Kafka 消息 key 过滤同一 topic 中的消息？

将 `format` 设为 `"NATIVE"` 可以把 Kafka 原始元数据（包括 `key` 字段）作为记录的一部分暴露出来。再用 SQL Transform 保留所需 key 值的消息：

```hocon
source {
  Kafka {
    topic = "events"
    bootstrap.servers = "localhost:9092"
    format = "NATIVE"
    consumer.group = "my-group"
  }
}
transform {
  Sql {
    plugin_input = "kafka_source"
    plugin_output = "filtered"
    query = "SELECT * FROM kafka_source WHERE key = 'expected_key_base64'"
  }
}
```

注意：NATIVE 格式中 `key` 字段为 base64 编码的字节数组。

### Kafka Source 支持哪些消息格式？

支持：`json`、`text`、`canal_json`、`debezium_json`、`ogg_json`、`avro`、`protobuf` 和 `NATIVE`。当需要将 Kafka 元数据（headers、key、partition、timestamp）作为记录字段使用时，选择 `NATIVE` 格式。

`format = avro` 默认读取原始 Avro 二进制消息。对于由 Confluent `KafkaAvroSerializer` 写入的消息，请设置 `strip_schema_registry_header = true` 并提供 `avro_schema`。连接器通过开头的 magic byte 检测并剥离固定的 5 字节线上格式头（magic byte `0` 加 4 字节 schema ID）后再解码，不会查询 Schema Registry。该选项默认关闭，关闭时原始 Avro 行为保持不变。

### 如何配置 SASL/Kerberos 认证？

通过 `kafka.*` 属性传入认证参数：

```hocon
source {
  Kafka {
    bootstrap.servers = "broker:9092"
    topic = "secure-topic"
    consumer.group = "my-group"
    kafka.security.protocol = "SASL_PLAINTEXT"
    kafka.sasl.mechanism = "GSSAPI"
    kafka.sasl.kerberos.service.name = "kafka"
    kafka.sasl.jaas.config = """com.sun.security.auth.module.Krb5LoginModule required
      useKeyTab=true
      keyTab="/etc/kafka/kafka.keytab"
      principal="user@REALM.COM";"""
  }
}
```

### 消费组 offset 是如何提交的？

SeaTunnel 在 checkpoint 完成时向 Kafka 提交 offset。需在 `env` 块中通过 `checkpoint.interval` 开启 checkpoint。使用 `start_mode = "group_offsets"` 重启任务时，将从上次 checkpoint 提交的 offset 恢复消费。

## 变更日志

<ChangeLog />
