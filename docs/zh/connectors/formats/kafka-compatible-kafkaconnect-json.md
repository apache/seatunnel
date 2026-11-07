# Kafka source 兼容 kafka-connect-json

Seatunnel 的 Kafka 连接器支持解析通过 Kafka Connect Source 抽取的数据，特别是从 Kafka Connect JDBC 和 Kafka Connect Debezium 抽取的数据

## 限制

更新消息必须包含完整行数据；仅包含变更字段的部分更新暂不支持。

墓碑消息（tombstone，即带有键且值为 `null` 的记录）不会产生任何行。Source 会跳过该记录并推进 offset，因此它不会被视为解析错误。
值存在但长度为 0 的记录属于另一种情况，仍然是解码错误。

# 格式选项

|              选项              |  默认值  | 是否需要 |                     描述                      |
|------------------------------|------|------|---------------------------------------------|
| key_converter_schema_enabled   | true | 否   | Kafka Connect key 转换后的 JSON 载荷是否携带 schema。   |
| value_converter_schema_enabled | true | 否   | Kafka Connect value 转换后的 JSON 载荷是否携带 schema。 |

> **注意：** 使用默认值（`true`）时，读取端期望收到 Kafka Connect JSON 信封（`{"schema": ..., "payload": ...}`）。如果数据是不带 schema 信封的普通 JSON，请将对应选项设置为 `false`。

# 如何使用

## Kafka 流入 Mysql

```bash
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "jdbc_source_record"
    plugin_output = "kafka_table"
    start_mode = earliest
    schema = {
      fields {
           id = "int"
           name = "string"
           description = "string"
           weight = "string"
      }
    },
    format = COMPATIBLE_KAFKA_CONNECT_JSON
  }
}


sink {
    Jdbc {
        driver = com.mysql.cj.jdbc.Driver
        url = "jdbc:mysql://localhost:3306/seatunnel"
        user = st_user
        password = seatunnel
        generate_sink_sql = true
        database = seatunnel
        table = jdbc_sink
        primary_keys = ["id"]
    }
}
```

