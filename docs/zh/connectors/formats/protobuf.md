# Protobuf 格式

Protobuf（Protocol Buffers）是一种由Google开发的语言中立、平台无关的数据序列化格式。它提供了一种高效的方式来编码结构化数据，同时支持多种编程语言和平台。

目前支持在 Kafka 中使用 protobuf 格式。

## 字段名匹配

Schema 字段优先按完全相同的名称匹配 proto 字段；没有精确匹配时，再使用与语言环境无关的大小写不敏感匹配。例如，`C_String` 可以匹配 `string c_string = 6;`，读取与写入使用相同规则。若同一个消息中有两个仅大小写不同的 proto 字段（例如 `foo` 与 `FOO`），而 schema 字段与两者都不完全相同（例如 `Foo`），则解析为最先声明的那个字段。

嵌套 `ROW` 的子字段按对应 proto 字段的消息类型解析，消息类型名称不必与列名相同；`MAP` 按所属字段的 map entry 描述符解析。写入非空值时，若 schema 字段没有对应的 proto 字段，作业会报错；读取时未匹配的列为 `null`。以前未匹配、现在按大小写不敏感规则匹配上的列，读取到的是字段值而不是 `null`，升级时请参考对应版本的不兼容变更说明。

`ARRAY` 列只支持非嵌套的元素类型，即 protobuf 标量类型和枚举；当前 SeaTunnel 转换器尚不支持 `ARRAY<ROW>` 和 `ARRAY<MAP>` 的元素转换。

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

## 墓碑消息与空消息

墓碑消息是只有 key、没有 value 的 Kafka 记录，压缩主题用它标记某个 key 已被删除。Protobuf 源端把这类记录读取为不产生任何数据行的记录：
它不属于格式错误，因此无论 `format_error_handle_way` 取何值都会被跳过，并且位点会继续向前推进。

空（长度为 0）的 value 不同。Protobuf 将其定义为每个字段都取默认值的合法消息，因此会被解码为由这些默认值组成的数据行，而不会被跳过。
