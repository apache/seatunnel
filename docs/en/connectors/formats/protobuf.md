# Protobuf Format

Protobuf (Protocol Buffers) is a language-neutral, platform-independent data serialization format developed by Google. It provides an efficient way to encode structured data and supports multiple programming languages and platforms.

Currently, Protobuf format can be used with Kafka.

## Kafka Usage Example

- Example of simulating a randomly generated data source and writing it to Kafka in Protobuf format

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
   FakeSource {
      parallelism = 1
      plugin_output = "fake"
      row.num = 16
      schema = {
        fields {
          c_int32 = int
          c_int64 = long
          c_float = float
          c_double = double
          c_bool = boolean
          c_string = string
          c_bytes = bytes

          Address {
              city = string
              state = string
              street = string
          }
          attributes = "map<string,float>"
          phone_numbers = "array<string>"
        }
      }
    }
}

sink {
  kafka {
      topic = "test_protobuf_topic_fake_source"
      bootstrap.servers = "kafkaCluster:9092"
      format = protobuf
      kafka.request.timeout.ms = 60000
      kafka.config = {
        acks = "all"
        request.timeout.ms = 60000
        buffer.memory = 33554432
      }
      protobuf_message_name = Person
      protobuf_schema = """
              syntax = "proto3";

              package org.apache.seatunnel.format.protobuf;

              option java_outer_classname = "ProtobufE2E";

              message Person {
                int32 c_int32 = 1;
                int64 c_int64 = 2;
                float c_float = 3;
                double c_double = 4;
                bool c_bool = 5;
                string c_string = 6;
                bytes c_bytes = 7;

                message Address {
                  string street = 1;
                  string city = 2;
                  string state = 3;
                  string zip = 4;
                }

                Address address = 8;

                map<string, float> attributes = 9;

                repeated string phone_numbers = 10;
              }
              """
  }
}
```

- Example of reading data from Kafka in Protobuf format and printing it to the console

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
    Kafka {
        topic = "test_protobuf_topic_fake_source"
        format = protobuf
        protobuf_message_name = Person
        protobuf_schema = """
            syntax = "proto3";

            package org.apache.seatunnel.format.protobuf;

            option java_outer_classname = "ProtobufE2E";

            message Person {
                int32 c_int32 = 1;
                int64 c_int64 = 2;
                float c_float = 3;
                double c_double = 4;
                bool c_bool = 5;
                string c_string = 6;
                bytes c_bytes = 7;

                message Address {
                    string street = 1;
                    string city = 2;
                    string state = 3;
                    string zip = 4;
                }

                Address address = 8;

                map<string, float> attributes = 9;

                repeated string phone_numbers = 10;
            }
        """
        schema = {
            fields {
                c_int32 = int
                c_int64 = long
                c_float = float
                c_double = double
                c_bool = boolean
                c_string = string
                c_bytes = bytes

                Address {
                    city = string
                    state = string
                    street = string
                }
                attributes = "map<string,float>"
                phone_numbers = "array<string>"
            }
        }
        bootstrap.servers = "kafkaCluster:9092"
        start_mode = "earliest"
        plugin_output = "kafka_table"
    }
}

sink {
  Console {
    plugin_input = "kafka_table"
  }
}
```

## Tombstone and empty messages

A Kafka tombstone is a record whose value is `null`, used on a compacted topic to mark its key as
deleted. The Protobuf source reads such a record as one that produces no row: it is not a format
error, so it is passed over whatever `format_error_handle_way` is set to, and the offset moves on
past it.

An empty (zero length) value is different. Protobuf defines it as a valid message in which every
field holds its default value, so it is decoded into a row of those defaults rather than being
passed over.
