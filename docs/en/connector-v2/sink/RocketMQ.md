# RocketMQ

> RocketMQ sink connector

## Support Apache RocketMQ Version

- 4.9.0 (Or a newer version, for reference)

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we will use 2pc to guarantee the message is sent to RocketMQ exactly once.

## Description

Write Rows to a Apache RocketMQ topic.

## Sink Options

|         Name         |  Type   | Required |         Default          |                                                                             Description                                                                             |
|----------------------|---------|----------|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                | string  | yes      | -                        | `RocketMQ topic` name.                                                                                                                                              |
| name.srv.addr        | string  | yes      | -                        | `RocketMQ` name server cluster address.                                                                                                                             |
| acl.enabled          | Boolean | no       | false                    | false                                                                                                                                                               |
| access.key           | String  | no       |                          | When ACL_ENABLED is true, access key cannot be empty                                                                                                                |
| secret.key           | String  | no       |                          | When ACL_ENABLED is true, secret key cannot be empty                                                                                                                |
| producer.group       | String  | no       | SeaTunnel-producer-Group | SeaTunnel-producer-Group                                                                                                                                            |
| partition.key.fields | array   | no       | -                        | -                                                                                                                                                                   |
| format               | String  | no       | json                     | Data format. The default format is json. Optional text format. The default field separator is ",".If you customize the delimiter, add the "field_delimiter" option. |
| field.delimiter      | String  | no       | ,                        | Customize the field delimiter for data format.                                                                                                                      |
| producer.send.sync   | Boolean | no       | false                    | If true, the message will be sync sent.                                                                                                                             |
| common-options       | config  | no       | -                        | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.                                                        |

### partition.key.fields [array]

Configure which fields are used as the key of the RocketMQ message.

For example, if you want to use value of fields from upstream data as key, you can assign field names to this property.

Upstream data is the following:

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

If name is set as the key, then the hash value of the name column will determine which partition the message is sent to.

## Task Example

### Fake to Rocketmq Simple

> The data is randomly generated and asynchronously sent to the test topic

```hocon
env {
  parallelism = 1
}

source {
  FakeSource {
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
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/category/transform
}

sink {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topic = "test_topic"
  }
}

```

### Rocketmq To Rocketmq Simple

> Consuming Rocketmq writes to c_int field Hash number of partitions written to different partitions This is the default asynchronous way to write

```hocon
env {
  parallelism = 1
}

source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test_topic"
    result_table_name = "rocketmq_table"
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
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topic = "test_topic_sink"
    partition.key.fields = ["c_int"]
  }
}
```

### Timestamp consumption write Simple

> This is a stream consumption specified time stamp consumption, when there are new partitions added the program will refresh the perception and consumption at intervals, and write to another topic type

```hocon

env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test_topic"
    result_table_name = "rocketmq_table"
    start.mode = "CONSUME_FROM_FIRST_OFFSET"
    batch.size = "400"
    consumer.group = "test_topic_group"
    format = "json"
    format = json
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
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/category/transform
}
sink {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topic = "test_topic"
    partition.key.fields = ["c_int"]
    producer.send.sync = true
  }
}
```

