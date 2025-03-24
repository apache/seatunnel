# CDC Compatible Debezium-json

SeaTunnel supports to interpret cdc record as Debezium-JSON messages publish to mq(kafka) system.

This is useful in many cases to leverage this feature, such as compatible with the debezium ecosystem.

# How To Use

## MySQL-CDC Sink Kafka

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 15000
}

source {
  MySQL-CDC {
    plugin_output = "table1"

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
        # topic prefix
        database.server.name =  "mysql_cdc_1"
    }
  }
}

sink {
  Kafka {
    plugin_input = "table1"

    bootstrap.servers = "localhost:9092"
    topic = "${topic}"

    # compatible_debezium_json options
    format = compatible_debezium_json
  }
}
```

