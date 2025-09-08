# Protobuf 格式

Protobuf（Protocol Buffers）是一种由Google开发的语言中立、平台无关的数据序列化格式。它提供了一种高效的方式来编码结构化数据，同时支持多种编程语言和平台。

目前支持在 Kafka 中使用 protobuf 格式。

## Kafka 使用示例

- 模拟随机生成数据源,并以 protobuf 的格式 写入 kafka 的实例

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

- 从 kafka 读取 protobuf 格式的数据并打印到控制台的示例

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

