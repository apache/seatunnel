# Kafka source 兼容 kafka-connect-json

Seatunnel 的 Kafka 连接器支持解析通过 Kafka Connect Source 抽取的数据，特别是从 Kafka Connect JDBC 和 Kafka Connect Debezium 抽取的数据

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

