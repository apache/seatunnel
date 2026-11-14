import ChangeLog from '../changelog/connector-rocketmq.md';

# RocketMQ

> RocketMQ sink connector

## Support Apache RocketMQ Version

- 4.9.0 or newer

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Writes SeaTunnel rows to an Apache RocketMQ topic. The sink supports JSON and text message bodies, optional message tags, synchronous sending, partition key fields, and transactional messages when `exactly.once = true`.

## Sink Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| topic | String | yes | - | RocketMQ topic name. |
| name.srv.addr | String | yes | - | RocketMQ name server address, for example `localhost:9876`. |
| acl.enabled | Boolean | no | false | Whether to enable RocketMQ ACL authentication. |
| access.key | String | no | - | Access key. Required when `acl.enabled` is `true`. |
| secret.key | String | no | - | Secret key. Required when `acl.enabled` is `true`. |
| producer.group | String | no | SeaTunnel-Producer-Group | RocketMQ producer group ID. |
| tag | String | no | - | RocketMQ message tag written with each message. |
| partition.key.fields | List | no | - | Field names serialized as the RocketMQ message key. Every listed field must exist in the upstream schema. |
| format | String | no | json | Message format. Supported values are `json` and `text`. |
| field.delimiter | String | no | `,` | Field delimiter used when `format = text`. |
| producer.send.sync | Boolean | no | false | Whether to send messages synchronously. When `false`, messages are sent asynchronously. |
| exactly.once | Boolean | no | false | Whether to send messages via a RocketMQ transactional producer. |
| max.message.size | int | no | 4194304 | Maximum message body size in bytes. |
| send.message.timeout | int | no | 3000 | Send timeout in milliseconds. |
| common-options | config | no | - | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md). |

## Option Notes

### partition.key.fields

`partition.key.fields` controls the RocketMQ message key. SeaTunnel serializes
the configured field values as JSON and writes that value to `Message.keys`. For
non-transactional sends, the same key is also used by RocketMQ's hash queue
selector, so rows with the same key are sent to the same queue. If this option is
not set, RocketMQ chooses the queue.

For example, if the input schema contains `c_int`, this configuration uses `c_int` to build the message key:

```hocon
partition.key.fields = ["c_int"]
```

### exactly.once

When enabled, the sink sends messages through a RocketMQ transactional producer. This behavior is disabled by default. Note that each transactional message is committed immediately after it is sent — the sink does not hold half-messages until the engine checkpoint completes, so this option enables transactional sending rather than checkpoint-based exactly-once delivery.

When `format = text`, SeaTunnel serializes fields in the upstream schema order and joins them with `field.delimiter`. When `format = json`, each row is written as a JSON object.

### producer.send.sync

`producer.send.sync = true` makes the producer wait for RocketMQ to acknowledge
each send request. With the default value `false`, messages are sent
asynchronously.

## Task Examples

### Write JSON Messages

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        c_string = string
        c_int = int
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_topic"
    partition.key.fields = ["c_int"]
    producer.send.sync = true
  }
}
```

### Write Text Messages

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        id = bigint
        content = string
      }
    }
  }
}

sink {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_text_topic"
    format = text
    field.delimiter = ","
    producer.send.sync = true
  }
}
```

### Write Messages With a Tag

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        c_string = string
        c_int = int
      }
    }
  }
}

sink {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_topic_message_tag"
    tag = "test_tag"
    partition.key.fields = ["c_string"]
    producer.send.sync = true
  }
}
```

### Read and Write RocketMQ

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_source"
    plugin_output = "rocketmq_table"
    format = json
    start.mode = "CONSUME_FROM_FIRST_OFFSET"
    consumer.group = "rocketmq_to_rocketmq_group"
    schema = {
      fields {
        id = bigint
        c_string = string
      }
    }
  }
}

sink {
  Rocketmq {
    plugin_input = "rocketmq_table"
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_topic_sink"
    partition.key.fields = ["id"]
    exactly.once = true
  }
}
```

## FAQ

### How can message routing and partition keys be configured in RocketMQ sink?

Configure `partition.key.fields` with one or more table field names. The connector calculates a hash of the specified fields to ensure that messages sharing identical key values are always routed to the same RocketMQ message queue.

### Does RocketMQ sink provide exactly-once delivery guarantees?

No. When `exactly.once = true`, the sink sends messages through a RocketMQ transactional producer, but each transactional message is committed immediately after sending — messages become visible to consumers right away and are not held until the engine checkpoint completes. The sink does not participate in checkpoint barrier coordination, so it does not provide checkpoint-based exactly-once delivery.

### What message serialization formats are supported?

The sink supports `json` and `text` formats. Fields can be formatted into JSON payloads or delimited text rows according to your target consumer requirements.

## Changelog

<ChangeLog />
