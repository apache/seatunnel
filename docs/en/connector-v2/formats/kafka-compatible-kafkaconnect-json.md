# Kafka source compatible kafka-connect-json

Seatunnel connector kafka supports parsing data extracted through kafka connect source, especially data extracted from kafka connect jdbc and kafka connect debezium

# How to use

## Kafka output to mysql

```bash
env {
    execution.parallelism = 1
    job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "jdbc_source_record"
    result_table_name = "kafka_table"
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

