# CDC 兼容 Debezium-json

SeaTunnel 支持将 cdc 记录解析为 Debezium-JSON 消息，并发布到 MQ (kafka) 等消息系统中

这个特性在很多场景下都非常实用，例如，它可以实现与 Debezium 生态系统的兼容性

# 如何使用

## MySQL-CDC 流入 Kafka

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 15000
}

source {
  MySQL-CDC {
    result_table_name = "table1"

    base-url="jdbc:mysql://localhost:3306/test"
    "startup.mode"=INITIAL
    table-names=[
        "database1.t1",
        "database1.t2",
        "database2.t1"
    ]

    # compatible_debezium_json options
    format = compatible_debezium_json
    debezium = {
        # include schema into kafka message
        key.converter.schemas.enable = false
        value.converter.schemas.enable = false
        # include ddl
        include.schema.changes = true
        # topic prefix
        database.server.name =  "mysql_cdc_1"
    }
  }
}

sink {
  Kafka {
    source_table_name = "table1"

    bootstrap.servers = "localhost:9092"

    # compatible_debezium_json options
    format = compatible_debezium_json
  }
}
```

