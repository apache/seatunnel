# Avro format

Avro is very popular in streaming data pipeline. Now seatunnel supports Avro format in kafka connector.

:::note Confluent Schema Registry is not supported

This format decodes plain Avro binary directly using the `schema` you configure; it does not talk to a schema registry. Messages produced by a Confluent Schema Registry-aware serializer carry a 5-byte wire-format header (a magic byte followed by a 4-byte schema ID) before the Avro payload, and this format does not strip that header, so registry-produced messages cannot be read as-is. To consume Confluent-style Avro from Kafka, either write a custom deserialization schema that strips the header before decoding, or use a producer that writes plain Avro without the registry header. The Protobuf format has a comparable [`strip_schema_registry_header`](../source/Kafka.md) option; no Avro equivalent exists yet.

:::

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

