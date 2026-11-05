# Avro 格式

Avro 在流式数据处理管道中非常流行。现在seatunnel在kafka连接器中支持Avro格式

:::note 不支持 Confluent Schema Registry

该格式直接使用你配置的 `schema` 解码原始 Avro 二进制数据，不会与 Schema Registry 交互。消费 Confluent Schema Registry 消息时，请设置 `strip_schema_registry_header = true` 并提供 `avro_schema`。连接器通过开头的 magic byte 检测并剥离 5 字节线上格式头（1 字节 magic byte `0` + 4 字节 schema ID），不会查询 Schema Registry。该选项默认关闭，关闭时原始 Avro 行为保持不变。

:::

## 选项

| 名称              | 类型    | 是否必须 | 默认值 | 描述                                                                                                                              |
|-------------------|---------|----------|--------|-----------------------------------------------------------------------------------------------------------------------------------|
| avro_row_nullable | Boolean | 否       | false  | 是否将所有 ROW 字段写为可空的 Avro union，以便序列化值为 `null` 的 ROW。Kafka 与 Pulsar 的 source 和 sink 连接器均支持。写入端与读取端必须同时开启。 |

### avro_row_nullable

默认情况下 ROW 字段是一个裸 Avro record，无法表示 `null`，因此写入值为 `null` 的 ROW 会失败。将 `avro_row_nullable` 设为 `true` 后，每个 ROW 字段都会改为 `[null, record]` union。该开关适用于任意嵌套深度，也适用于嵌套在另一个 ROW、ARRAY 或 MAP 中的 ROW，因此值为 `null` 的 ROW 字段、嵌套 ROW 字段、ARRAY 元素和 MAP value 都可以写入。

由于该选项会改变所有包含 ROW 字段的 schema 的二进制布局，必须在写入端**和**读取端同时开启：只开启一侧的作业无法解码另一侧写出的消息。

写入作业配置：

```hocon
sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "test_avro_nullable_row"
    format = avro
    avro_row_nullable = true
  }
}

```

读取作业配置：

```hocon
source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "test_avro_nullable_row"
    format = avro
    avro_row_nullable = true
    schema = {
      fields {
        id = bigint
        c_row = {
          c_int = int
        }
      }
    }
  }
}
```

混合部署时仍可使用旧版读取路径：Kafka source 配置了 `avro_schema` 后，该 schema 始终是线上布局的权威描述，无论 `avro_row_nullable` 如何配置，消息都会按写入时的布局解码。参见 [Avro 反序列化](../source/Kafka.md#avro-反序列化)。

:::note 原始 Avro 字节不自描述

SeaTunnel 使用你配置的 `schema` 解码原始 Avro 字节，没有 schema 自动探测，也不会逐条消息查找 schema，因此读取端无法判断消息是用何种 `avro_row_nullable` 配置写入的。该选项既不负责调和两种布局，也不会迁移已经按默认布局写入的历史消息：这些消息在读取端仍需使用默认布局，或通过 `avro_schema` 提供其精确的 writer schema。

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
