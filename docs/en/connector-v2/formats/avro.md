# Avro format

Avro is very popular in streaming data pipeline. Now seatunnel supports Avro format in kafka connector.

# How To Use

## Kafka uses example

- This is an example to generate data from fake source and sink to kafka with avro format.

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
    result_table_name = "fake"
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

- This is an example read data from kafka with avro format and print to console.

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "test_avro_topic"
    result_table_name = "kafka_table"
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
    source_table_name = "kafka_table"
  }
}
```

