# Kafka source compatible kafka-connect-json

Seatunnel connector kafka supports parsing data extracted through kafka connect source, especially data extracted from kafka connect jdbc and kafka connect debezium

# Format Options

| Option                         | Default | Required | Description                                              |
|--------------------------------|---------|----------|----------------------------------------------------------|
| format                         | (none)  | yes      | Specify what format to use, here should be `compatible_kafka_connect_json`. |
| key_converter_schema_enabled   | true    | no       | Whether the Kafka Connect key converter carries its schema in the JSON payload. |
| value_converter_schema_enabled | true    | no       | Whether the Kafka Connect value converter carries its schema in the JSON payload. |

## Limitations

UPDATE messages must contain a complete row image; partial updates containing only changed fields are not supported.

A tombstone, a record that carries a key and a `null` value, produces no row. The source passes it over and the
offset advances, so it is not treated as a parse error. A record whose value is present but zero length is a
different case and is still a decode error.

# How To Use

## Kafka Sink Mysql

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

