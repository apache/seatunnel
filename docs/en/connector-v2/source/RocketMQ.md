# RocketMQ

> RocketMQ source connector

## Support Apache RocketMQ Version

- 4.9.0 (Or a newer version, for reference)

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Source connector for Apache RocketMQ.

## Source Options

|                Name                 |  Type   | Required |          Default           |                                                                                                    Description                                                                                                     |
|-------------------------------------|---------|----------|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topics                              | String  | yes      | -                          | `RocketMQ topic` name. If there are multiple `topics`, use `,` to split, for example: `"tpc1,tpc2"`.                                                                                                               |
| name.srv.addr                       | String  | yes      | -                          | `RocketMQ` name server cluster address.                                                                                                                                                                            |
| acl.enabled                         | Boolean | no       | false                      | If true, access control is enabled, and access key and secret key need to be configured.                                                                                                                           |
| access.key                          | String  | no       |                            |                                                                                                                                                                                                                    |
| secret.key                          | String  | no       |                            | When ACL_ENABLED is true, secret key cannot be empty.                                                                                                                                                              |
| batch.size                          | int     | no       | 100                        | `RocketMQ` consumer pull batch size                                                                                                                                                                                |
| consumer.group                      | String  | no       | SeaTunnel-Consumer-Group   | `RocketMQ consumer group id`, used to distinguish different consumer groups.                                                                                                                                       |
| commit.on.checkpoint                | Boolean | no       | true                       | If true the consumer's offset will be periodically committed in the background.                                                                                                                                    |
| schema                              |         | no       | -                          | The structure of the data, including field names and field types.                                                                                                                                                  |
| format                              | String  | no       | json                       | Data format. The default format is json. Optional text format. The default field separator is ",".If you customize the delimiter, add the "field.delimiter" option.                                                |
| field.delimiter                     | String  | no       | ,                          | Customize the field delimiter for data format                                                                                                                                                                      |
| start.mode                          | String  | no       | CONSUME_FROM_GROUP_OFFSETS | The initial consumption pattern of consumers,there are several types: [CONSUME_FROM_LAST_OFFSET],[CONSUME_FROM_FIRST_OFFSET],[CONSUME_FROM_GROUP_OFFSETS],[CONSUME_FROM_TIMESTAMP],[CONSUME_FROM_SPECIFIC_OFFSETS] |
| start.mode.offsets                  |         | no       |                            |                                                                                                                                                                                                                    |
| start.mode.timestamp                | Long    | no       |                            | The time required for consumption mode to be "CONSUME_FROM_TIMESTAMP".                                                                                                                                             |
| partition.discovery.interval.millis | long    | no       | -1                         | The interval for dynamically discovering topics and partitions.                                                                                                                                                    |
| ignore_parse_errors                 | Boolean | no       | false                      | Optional flag to skip parse errors instead of failing.                                                                                                                                                             |
| common-options                      | config  | no       | -                          | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                 |

### start.mode.offsets

The offset required for consumption mode to be "CONSUME_FROM_SPECIFIC_OFFSETS".

for example:

```hocon
start.mode.offsets = {
  topic1-0 = 70
  topic1-1 = 10
  topic1-2 = 10
}
```

## Task Example

### Simple:

> Consumer reads Rocketmq data and prints it to the console type

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_json"
    plugin_output = "rocketmq_table"
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

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/category/transform
}

sink {
  Console {
  }
}
```

### Specified format consumption Simple:

> When I consume the topic data in json format parsing and pulling the number of bars each time is 400, the consumption starts from the original location

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test_topic"
    plugin_output = "rocketmq_table"
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
  Console {
  }
}
```

### Specified timestamp Simple:

> This is to specify a time to consume, and I dynamically sense the existence of a new partition every 1000 milliseconds to pull the consumption

```hocon
env {
  parallelism = 1
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test_topic"
    partition.discovery.interval.millis = "1000"
    start.mode.timestamp="1694508382000"
    consumer.group="test_topic_group"
    format="json"
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
  Console {
  }
}
```

