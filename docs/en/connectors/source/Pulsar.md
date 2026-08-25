import ChangeLog from '../changelog/connector-pulsar.md';

# Apache Pulsar

> Apache Pulsar source connector

## Description

Source connector for Apache Pulsar.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Options

| Name                     | Type    | Required | Default Value | Description                                                                                                      |
|--------------------------|---------|----------|---------------|------------------------------------------------------------------------------------------------------------------|
| topic                    | String  | No       | -             | Topic name(s) to read. Supports comma-separated list. **Note: only one of `topic`, `topic-pattern`, `tables_configs`** |
| topic-pattern            | String  | No       | -             | Regular expression for topic names. **Note: only one of `topic`, `topic-pattern`, `tables_configs`**            |
| table_path               | String  | No       | -             | Logical table identifier for multi-table mode                                                                    |
| tables_configs           | Array   | No       | -             | Multi-table configuration. Each item can override global defaults. **Note: only one of `topic`, `topic-pattern`, `tables_configs`** |
| topic-discovery.interval | Long    | No       | -1            | Interval (ms) to discover new partitions. Non-positive disables discovery. Only works with `topic-pattern`      |
| subscription.name        | String  | Required for single-table mode or per multi-table item | - | Consumer subscription name. Can be defined globally or per item in multi-table mode                              |
| client.service-url       | String  | Yes      | -             | Pulsar client service URL, e.g., `pulsar://localhost:6650`                                                      |
| admin.service-url        | String  | Yes      | -             | Pulsar admin HTTP URL, e.g., `http://localhost:8080`                                                            |
| auth.plugin-class        | String  | No       | -             | Pulsar client authentication plugin class name                                                                   |
| auth.params              | String  | No       | -             | Pulsar client authentication parameters                                                                          |
| poll.timeout             | Integer | No       | 100           | Timeout (ms) for polling messages from Pulsar                                                                    |
| poll.interval            | Long    | No       | 50            | Interval (ms) between two polls                                                                                  |
| poll.batch.size          | Integer | No       | 500           | Maximum number of messages to poll in a single batch                                                             |
| cursor.startup.mode      | Enum    | No       | LATEST        | Startup position mode. Options: `EARLIEST`, `LATEST`, `SUBSCRIPTION`, `TIMESTAMP`                                |
| cursor.startup.timestamp | Long    | No       | -             | Start timestamp (ms) when `cursor.startup.mode=TIMESTAMP`                                                        |
| cursor.reset.mode        | Enum    | Required when `cursor.startup.mode=SUBSCRIPTION` | - | Reset mode when `cursor.startup.mode=SUBSCRIPTION`. Options: `EARLIEST`, `LATEST`                               |
| cursor.stop.mode         | Enum    | No       | NEVER         | Stop position mode. Options: `NEVER` (streaming), `LATEST` (batch), `TIMESTAMP` (batch)                         |
| cursor.stop.timestamp    | Long    | No       | -             | Stop timestamp (ms) when `cursor.stop.mode=TIMESTAMP`                                                            |
| schema                   | Config  | No       | -             | Data structure including field names and types                                                                   |
| format                   | String  | No       | json          | Data format. Default is json. Supported formats: json, canal_json, avro and text. **Text is supported only in single-table mode; multi-table mode supports JSON, CANAL_JSON and AVRO** |
| field_delimiter          | String  | No       | ,             | Field delimiter for `text` format.                                                                               |
| common-options           |         | No       | -             | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md) for details           |

### topic [String]

Topic name(s) to read data from when the table is used as source. It also supports topic lists by separating topics with commas like `'topic-1,topic-2'`.

**Note, only one of `topic`, `topic-pattern` and `tables_configs` can be specified for sources.**

### topic-pattern [String]

The regular expression for a pattern of topic names to read from. All topics with names that match the specified regular expression will be subscribed by the consumer when the job starts running.

**Note, only one of `topic`, `topic-pattern` and `tables_configs` can be specified for sources.**

### table_path [String]

Logical table identifier for one `tables_configs` item. This option is mainly used in multi-table mode.

### tables_configs [Array]

Multi-table source configuration. Each item can override global defaults such as `format`, cursor options and `subscription.name`.

Each item must configure exactly one of:

- `topic`
- `topic-pattern`

Additional rules:

- `table_path` is required when `topic-pattern` is used.
- `subscription.name` must exist either globally or inside the item.
- Only `JSON`, `CANAL_JSON` and `AVRO` are supported in multi-table mode.
- Explicit `topic` entries must not overlap with any `topic-pattern` entry.
- If multiple `topic-pattern` items can match the same topic, the first matching item in `tables_configs` wins. Put more specific patterns before broader ones.
- In batch mode, multi-table configurations must be bounded. If more than one table is configured and any table uses `cursor.stop.mode = NEVER`, the source is unbounded and batch jobs are rejected. Single-table mode and single-entry `tables_configs` keep backward-compatible batch behavior.

### topic-discovery.interval [Long]

The interval (in ms) for the Pulsar source to discover the new topic partitions. A non-positive value disables the topic partition discovery.

**Note, This option only works if the 'topic-pattern' option is used.**

### subscription.name [String]

Specify the subscription name for this consumer.

For a single-table source, `subscription.name` is required. For a multi-table source, it can be defined globally or inside each `tables_configs` item. If it is configured in both places, the item-level value takes effect for that item.

### client.service-url [String]

Service URL provider for Pulsar service.
To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.
You can assign Pulsar protocol URLs to specific clusters and use the Pulsar scheme.

For example, `localhost`: `pulsar://localhost:6650,localhost:6651`.

### admin.service-url [String]

The Pulsar service HTTP URL for the admin endpoint.

For example, `http://my-broker.example.com:8080`, or `https://my-broker.example.com:8443` for TLS.

### auth.plugin-class [String]

Name of the authentication plugin.

### auth.params [String]

Parameters for the authentication plugin.

For example, `key1:val1,key2:val2`

### poll.timeout [Integer]

The maximum time (in ms) to wait when fetching records. A longer time increases throughput but also latency.

### poll.interval [Long]

The interval time (in ms) when fetching records. A shorter time increases throughput, but also increases CPU load.

### poll.batch.size [Integer]

The maximum number of records to fetch in one poll.

### cursor.startup.mode [Enum]

Startup mode for Pulsar consumer, valid values are `'EARLIEST'`, `'LATEST'`, `'SUBSCRIPTION'`, `'TIMESTAMP'`.

### cursor.startup.timestamp [Long]

Start from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "cursor.startup.mode" option used `'TIMESTAMP'`.**

### cursor.reset.mode [Enum]

Cursor reset strategy for Pulsar consumer valid values are `'EARLIEST'`, `'LATEST'`.

**Note, This option only works if the "cursor.startup.mode" option used `'SUBSCRIPTION'`.**
It has no default value and must be configured in that mode.

### cursor.stop.mode [String]

Stop mode for Pulsar consumer, valid values are `'NEVER'`, `'LATEST'`and `'TIMESTAMP'`.

**Note, When `'NEVER' `is specified, it is a real-time job, and other mode are off-line jobs.**

### cursor.stop.timestamp [Long]

Stop from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "cursor.stop.mode" option used `'TIMESTAMP'`.**

### schema [Config]

The structure of the data, including field names and field types.
reference to [Schema-Feature](../../introduction/concepts/schema-feature.md)

### format [String]

Data format. The default format is json. Supported formats are json, canal_json, avro and text. The `schema` option is required when using avro format. Text format is supported only in single-table mode. See [formats](../formats) for more details.

### field_delimiter [String]

Field delimiter for `text` format. The default value is `,`.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

### Single-Topic Batch Read

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Pulsar {
    topic = "topic-it"
    subscription.name = "seatunnel"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "LATEST"
    format = json
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_timestamp = timestamp
      }
    }
  }
}
```

### Read Text Messages

Use `format = text` in single-table mode to split each message into schema fields with `field_delimiter`.

```hocon
source {
  Pulsar {
    topic = "text-events"
    subscription.name = "seatunnel-text-sub"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "LATEST"
    format = text
    field_delimiter = "|"
    schema = {
      fields {
        id = int
        name = string
      }
    }
  }
}
```

### Read Canal JSON Messages

Use `format = canal_json` when the Pulsar topic stores Canal JSON change events.

```hocon
source {
  Pulsar {
    topic = "test-cdc_mds"
    subscription.name = "seatunnel-cdc-sub"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "LATEST"
    format = canal_json
    schema = {
      fields {
        id = int
        name = string
        description = string
        weight = string
      }
    }
  }
}
```

### Multi-Table Read

```hocon
source {
  Pulsar {
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "LATEST"
    format = "json"

    tables_configs = [
      {
        table_path = "db.orders"
        topic = "persistent://public/default/orders"
        subscription.name = "sub-orders"
        schema = {
          fields {
            order_id = int
            amount = double
          }
        }
      },
      {
        table_path = "db.users"
        topic-pattern = "persistent://public/default/users-.*"
        subscription.name = "sub-users"
        schema = {
          fields {
            user_id = int
            name = string
          }
        }
      }
    ]
  }
}
```

For batch jobs, use a bounded stop mode such as `LATEST` or `TIMESTAMP`. Use `cursor.stop.mode = "NEVER"` for streaming jobs.

### Read Avro Messages

When the topic carries Avro-encoded records, set `format = avro` and declare the field types in `schema`. The connector decodes the Avro payload using the SeaTunnel type system.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Pulsar {
    topic = "test_avro_topic"
    subscription.name = "seatunnel-avro"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "LATEST"
    format = avro
    schema = {
      fields {
        id = bigint
        c_string = string
        c_int = int
        c_double = double
        c_timestamp = timestamp
      }
    }
  }
}
```

### Streaming Read From a Topic

Use `cursor.stop.mode = "NEVER"` to keep reading new messages until the job is stopped. Pair it with `cursor.startup.mode = "LATEST"` to start from the latest message and avoid replaying history.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Pulsar {
    topic = "persistent://public/default/events"
    subscription.name = "seatunnel-stream"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "LATEST"
    cursor.stop.mode = "NEVER"
    format = json
    schema = {
      fields {
        event_id = string
        user_id = bigint
        payload = string
      }
    }
  }
}
```

### Read From a Topic Pattern With Discovery

When the topic list grows over time, combine `topic-pattern` with `topic-discovery.interval` to pick up newly created topics automatically.

```hocon
source {
  Pulsar {
    topic-pattern = "persistent://public/default/orders-.*"
    subscription.name = "seatunnel-orders"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    topic-discovery.interval = 30000
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "LATEST"
    format = json
    schema = {
      fields {
        order_id = bigint
        amount = double
      }
    }
  }
}
```

## Changelog

<ChangeLog />
