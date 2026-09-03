# Avro 格式

Avro 在流式数据处理管道中非常流行。现在seatunnel在kafka连接器中支持Avro格式

:::note 不支持 Confluent Schema Registry

该格式直接使用你配置的 `schema` 解码原始 Avro 二进制数据，不会与 schema registry 交互。经过 Confluent Schema Registry 序列化的消息，会在 Avro 数据前带有 5 字节的线上格式头（1 字节 magic byte + 4 字节 schema ID），该格式不会剥离这部分头信息，因此无法直接读取 registry 生成的消息。如果需要消费 Confluent 风格的 Avro 数据，可以自行实现一个在解码前剥离头信息的反序列化 schema，或者改用不添加 registry 头的生产者写入普通 Avro 数据。Protobuf 格式提供了类似的 [`strip_schema_registry_header`](../source/Kafka.md) 选项；Avro 目前还没有对应的选项。

:::

# 怎样用

## Kafka 使用示例

- 模拟随机生成数据源,并以 Avro 的格式 写入 Kafka 的实例

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 90
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(38, 18)"
        c_timestamp = timestamp
        c_row = {
          c_map = "map<string, string>"
          c_array = "array<int>"
          c_string = string
          c_boolean = boolean
          c_tinyint = tinyint
          c_smallint = smallint
          c_int = int
          c_bigint = bigint
          c_float = float
          c_double = double
          c_bytes = bytes
          c_date = date
          c_decimal = "decimal(38, 18)"
          c_timestamp = timestamp
        }
      }
    }
    plugin_output = "fake"
  }
}

sink {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "test_avro_topic_fake_source"
    format = avro
  }
}
```

- 从 kafka 读取 avro 格式的数据并打印到控制台的示例

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "test_avro_topic"
    plugin_output = "kafka_table"
    start_mode = "earliest"
    format = avro
    format_error_handle_way = skip
    schema = {
      fields {
        id = bigint
        c_map = "map<string, smallint>"
        c_array = "array<tinyint>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(2, 1)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Console {
    plugin_input = "kafka_table"
  }
}
```

