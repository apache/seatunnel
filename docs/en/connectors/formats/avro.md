# Avro format

Avro is very popular in streaming data pipeline. Now seatunnel supports Avro format in kafka connector.

:::note Confluent Schema Registry is not supported

This format decodes plain Avro binary directly using the `schema` you configure; it does not talk to a schema registry. For Confluent Schema Registry messages consumed through Kafka, set `strip_schema_registry_header = true` and provide `avro_schema`. The connector detects the header by its leading magic byte and strips the five-byte wire header (magic byte `0` plus four-byte schema ID) without contacting Schema Registry. The option is opt-in and raw Avro behavior is unchanged when disabled.

:::

## Options

| Name              | Type    | Required | Default | Description                                                                                                                                                                                                                                          |
|-------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| avro_row_nullable | Boolean | No       | false   | Whether every ROW field is written as a nullable Avro union so a `null` ROW value can be serialized. Supported by the Kafka and Pulsar source and sink connectors. Must be enabled on the writer and the reader alike. |

### avro_row_nullable

By default a ROW field is a bare Avro record, which has no representation for `null`, so writing a `null` ROW value fails. Set `avro_row_nullable = true` to describe every ROW field as the union `[null, record]` instead. The flag applies at any nesting depth and to a ROW that is nested in another ROW, in an ARRAY or in a MAP, so a `null` ROW field, a `null` nested ROW field, a `null` ARRAY element and a `null` MAP value can all be written.

Because the option changes the binary layout of every schema that contains a ROW field, enable it on the writer **and** on the reader: a job that enables only one side cannot decode the other side's messages.

Writer job configuration:

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

Reader job configuration:

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

A legacy reader path stays open for mixed deployments: when the Kafka source is given the writer's `avro_schema`, that schema stays authoritative for the wire layout and the messages are decoded exactly as they were written, whatever `avro_row_nullable` says. See [Avro Deserialization](../source/Kafka.md#avro-deserialization).

:::note Raw Avro bytes are not self-describing

SeaTunnel decodes raw Avro bytes with the `schema` you configure; there is no schema autodetection and no per-message schema lookup, so a reader cannot tell whether the messages were written with or without `avro_row_nullable`. The option does not reconcile the two layouts and does not migrate messages that were already written with the default layout: those still need the default layout, or their exact writer schema via `avro_schema`, on the reader side.

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
