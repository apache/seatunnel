# Kafka

> Kafka 源连接器

## 支持以下引擎

> Spark<br/>  
> Flink<br/>  
> Seatunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义拆分](../../concept/connector-v2-features.md)

## 描述

用于 Apache Kafka 的源连接器。

## 支持的数据源信息

使用 Kafka 连接器需要以下依赖项。  
可以通过 install-plugin.sh 下载或从 Maven 中央仓库获取。

| 数据源   | 支持的版本 | Maven 下载链接                                                                    |
|-------|-------|-------------------------------------------------------------------------------|
| Kafka | 通用版本  | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-kafka) |

## 源选项

| 名称                                  | 类型                                  | 是否必填 | 默认值                      | 描述                                                                                                                                                                                                                                                                                                                      |
|-------------------------------------|-------------------------------------|------|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                               | String                              | 是    | -                        | 使用表作为数据源时要读取数据的主题名称。它也支持通过逗号分隔的多个主题列表，例如 'topic-1,topic-2'。                                                                                                                                                                                                                                                             |
| table_list                          | Map                                 | 否    | -                        | 主题列表配置，你可以同时配置一个 `table_list` 和一个 `topic`。                                                                                                                                                                                                                                                                              |
| bootstrap.servers                   | String                              | 是    | -                        | 逗号分隔的 Kafka brokers 列表。                                                                                                                                                                                                                                                                                                 |
| pattern                             | Boolean                             | 否    | false                    | 如果 `pattern` 设置为 `true`，则会使用指定的正则表达式匹配并订阅主题。                                                                                                                                                                                                                                                                            |
| consumer.group                      | String                              | 否    | SeaTunnel-Consumer-Group | `Kafka 消费者组 ID`，用于区分不同的消费者组。                                                                                                                                                                                                                                                                                            |
| commit_on_checkpoint                | Boolean                             | 否    | true                     | 如果为 true，消费者的偏移量将会定期在后台提交。                                                                                                                                                                                                                                                                                              |
| poll.timeout                        | Long                                | 否    | 10000                    | kafka主动拉取时间间隔(毫秒)。                                                                                                                                                                                                                                                                                                      |
| kafka.config                        | Map                                 | 否    | -                        | 除了上述必要参数外，用户还可以指定多个非强制的消费者客户端参数，覆盖 [Kafka 官方文档](https://kafka.apache.org/documentation.html#consumerconfigs) 中指定的所有消费者参数。                                                                                                                                                                                               |
| schema                              | Config                              | 否    | -                        | 数据结构，包括字段名称和字段类型。                                                                                                                                                                                                                                                                                                       |
| format                              | String                              | 否    | json                     | 数据格式。默认格式为 json。可选格式包括 text, canal_json, debezium_json, ogg_json, maxwell_json, avro 和 protobuf。默认字段分隔符为 ", "。如果自定义分隔符，添加 "field_delimiter" 选项。如果使用 canal 格式，请参考 [canal-json](../formats/canal-json.md) 了解详细信息。如果使用 debezium 格式，请参考 [debezium-json](../formats/debezium-json.md)。一些Format的详细信息请参考 [formats](../formats) |
| format_error_handle_way             | String                              | 否    | fail                     | 数据格式错误的处理方式。默认值为 fail，可选值为 fail 和 skip。当选择 fail 时，数据格式错误将阻塞并抛出异常。当选择 skip 时，数据格式错误将跳过此行数据。                                                                                                                                                                                                                              |
| debezium_record_table_filter        | Config                              | 否    | -                        | 用于过滤 debezium 格式的数据，仅当格式设置为 `debezium_json` 时使用。请参阅下面的 `debezium_record_table_filter`                                                                                                                                                                                                                                   |
| field_delimiter                     | String                              | 否    | ,                        | 自定义数据格式的字段分隔符。                                                                                                                                                                                                                                                                                                          |
| start_mode                          | StartMode[earliest],[group_offsets] | 否    | group_offsets            | 消费者的初始消费模式。                                                                                                                                                                                                                                                                                                             |
| start_mode.offsets                  | Config                              | 否    | -                        | 用于 specific_offsets 消费模式的偏移量。                                                                                                                                                                                                                                                                                           |
| start_mode.timestamp                | Long                                | 否    | -                        | 用于 "timestamp" 消费模式的时间。                                                                                                                                                                                                                                                                                                 |
| partition-discovery.interval-millis | Long                                | 否    | -1                       | 动态发现主题和分区的间隔时间。                                                                                                                                                                                                                                                                                                         |
| common-options                      |                                     | 否    | -                        | 源插件的常见参数，详情请参考 [Source Common Options](../source-common-options.md)。                                                                                                                                                                                                                                                    |
| protobuf_message_name               | String                              | 否    | -                        | 当格式设置为 protobuf 时有效，指定消息名称。                                                                                                                                                                                                                                                                                             |
| protobuf_schema                     | String                              | 否    | -                        | 当格式设置为 protobuf 时有效，指定 Schema 定义。                                                                                                                                                                                                                                                                                       |

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

## 任务示例

### 简单示例

> 此示例读取 Kafka 的 topic_1、topic_2 和 topic_3 的数据并将其打印到客户端。如果尚未安装和部署 SeaTunnel，请按照 [安装指南](../../start-v2/locally/deployment.md) 进行安装和部署。然后，按照 [快速开始](../../start-v2/locally/quick-start-seatunnel-engine.md) 运行此任务。

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
          pattern = "true" 
          bootstrap.servers = "localhost:9092"
          consumer.group = "seatunnel_group"
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
            java.security.krb5.conf="/etc/krb5.conf"
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
        pattern = "true"
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
        pattern = "true"
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
