import ChangeLog from '../changelog/connector-rocketmq.md';

# RocketMQ

> RocketMQ source connector

## Support Apache RocketMQ Version

- 4.9.0 or newer

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Description

Reads messages from Apache RocketMQ topics. The connector can read one or more topics with one schema, or use `tables_configs` to read multiple topics with different schemas.

## Source Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| name.srv.addr | String | yes | - | RocketMQ name server address, for example `localhost:9876`. |
| topics | String | no | - | Topic name list separated by commas, for example `"topic_a,topic_b"`. Configure only one of `topics`, `tables_configs`, and `table_list`. |
| tables_configs | List | no | - | Multi-table read configuration. Each item must contain `topics` and can contain `format`, `schema`, `tags`, `start.mode`, `start.mode.timestamp`, `start.mode.offsets`, and `ignore_parse_errors`. |
| table_list | List | no | - | Deprecated. Use `tables_configs` instead. |
| tags | String | no | - | Tag list separated by commas. Only messages with these tags are consumed. |
| acl.enabled | Boolean | no | false | Whether to enable RocketMQ ACL authentication. |
| access.key | String | no | - | Access key. Required when `acl.enabled` is `true`. |
| secret.key | String | no | - | Secret key. Required when `acl.enabled` is `true`. |
| batch.size | int | no | 100 | Maximum number of messages pulled each time. |
| consumer.group | String | no | SeaTunnel-Consumer-Group | RocketMQ consumer group ID. |
| commit.on.checkpoint | Boolean | no | true | Whether to commit offsets when SeaTunnel checkpoints are completed. |
| schema | config | no | - | Message schema. See [Schema Feature](../../introduction/concepts/schema-feature.md). If omitted, the connector reads message bodies as text. |
| format | String | no | json | Message format. Supported values are `json` and `text`. |
| field.delimiter | String | no | `,` | Field delimiter used when `format = text`. |
| start.mode | String | no | CONSUME_FROM_GROUP_OFFSETS | Startup position. Supported values: `CONSUME_FROM_LAST_OFFSET`, `CONSUME_FROM_FIRST_OFFSET`, `CONSUME_FROM_GROUP_OFFSETS`, `CONSUME_FROM_TIMESTAMP`, `CONSUME_FROM_SPECIFIC_OFFSETS`. |
| start.mode.offsets | Map | no | - | Required when `start.mode = CONSUME_FROM_SPECIFIC_OFFSETS`. The key format is `topic-queueId`, for example `test_topic-0`. |
| start.mode.timestamp | Long | no | - | Required when `start.mode = CONSUME_FROM_TIMESTAMP`. Use a millisecond timestamp. |
| partition.discovery.interval.millis | long | no | -1 | Topic and partition discovery interval in milliseconds. `-1` disables dynamic discovery. |
| ignore_parse_errors | Boolean | no | false | Whether to skip messages that cannot be parsed. |
| consumer.poll.timeout.millis | long | no | 5000 | Pull timeout in milliseconds. |
| common-options | config | no | - | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

## Option Notes

### Startup Position

`start.mode` controls where the source starts reading:

- `CONSUME_FROM_GROUP_OFFSETS`: start from committed offsets of the consumer group.
- `CONSUME_FROM_FIRST_OFFSET`: start from the earliest available offset.
- `CONSUME_FROM_LAST_OFFSET`: start from the latest available offset.
- `CONSUME_FROM_TIMESTAMP`: start from the first offset at or after `start.mode.timestamp`.
- `CONSUME_FROM_SPECIFIC_OFFSETS`: start from the offsets in `start.mode.offsets`.

```hocon
start.mode = "CONSUME_FROM_SPECIFIC_OFFSETS"
start.mode.offsets = {
  test_topic-0 = 50
}
```

### Multi-Table Read

Use `tables_configs` when different topics have different schemas. Each item must contain `topics` and can define its own `schema`, `format`, `tags`, and startup position. If `schema.table` is not set, the output table name defaults to the topic name.

`topics`, `tables_configs`, and the deprecated `table_list` are mutually exclusive. In `tables_configs`, options that are not set on an item inherit the top-level defaults, so each item only needs to override the topic-specific schema, tags, or startup position.

When a `tables_configs` item uses `start.mode = CONSUME_FROM_TIMESTAMP`, it must also set `start.mode.timestamp`. When it uses `start.mode = CONSUME_FROM_SPECIFIC_OFFSETS`, it must also set a non-empty `start.mode.offsets` map.

## Task Examples

### Read JSON Messages

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
    format = json
    schema = {
      fields {
        id = bigint
        c_string = string
        c_int = int
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Console {
    plugin_input = "rocketmq_table"
  }
}
```

### Read Text Messages With Tags

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_text"
    plugin_output = "rocketmq_table"
    format = text
    field.delimiter = ","
    tags = "tag_a,tag_b"
    schema = {
      fields {
        id = bigint
        content = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "rocketmq_table"
  }
}
```

### Read From Specific Offsets

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_source"
    plugin_output = "rocketmq_table"
    format = json
    start.mode = "CONSUME_FROM_SPECIFIC_OFFSETS"
    start.mode.offsets = {
      test_topic_source-0 = 50
    }
    schema = {
      fields {
        id = bigint
      }
    }
  }
}

sink {
  Console {
    plugin_input = "rocketmq_table"
  }
}
```

### Read Multiple Topics With Different Schemas

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    start.mode = "CONSUME_FROM_LAST_OFFSET"
    tables_configs = [
      {
        topics = "test_topic_multi_a"
        start.mode = "CONSUME_FROM_FIRST_OFFSET"
        format = json
        schema = {
          fields {
            id = bigint
            c_string = string
          }
        }
      },
      {
        topics = "test_topic_multi_b"
        start.mode = "CONSUME_FROM_FIRST_OFFSET"
        tags = "tag_b"
        format = json
        schema = {
          table = "rocketmq_multi_custom"
          fields {
            id = bigint
            description = string
          }
        }
      }
    ]
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
