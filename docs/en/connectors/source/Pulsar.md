import ChangeLog from '../changelog/connector-pulsar.md';

# Apache Pulsar

> Apache Pulsar source connector

## Support Those Engines

> SeaTunnel Zeta<br/>

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

## Source Options

| Name                     | Type    | Required                                          | Default Value | Description                                                                                                                                                                              |
|--------------------------|---------|---------------------------------------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                    | String  | No                                                | -             | Topic name(s) to read. Supports a comma-separated list. Only one of `topic`, `topic-pattern`, and `tables_configs` can be configured.                                                  |
| topic-pattern            | String  | No                                                | -             | Regular expression for topic names. Only one of `topic`, `topic-pattern`, and `tables_configs` can be configured.                                                                       |
| table_path               | String  | No                                                | -             | Logical table identifier used inside a `tables_configs` entry. Mainly used in multi-table mode.                                                                                         |
| tables_configs           | Array   | No                                                | -             | Multi-table configuration. Each item can override global defaults. Only one of `topic`, `topic-pattern`, and `tables_configs` can be configured.                                         |
| topic-discovery.interval | Long    | No                                                | -1            | Interval (ms) for dynamically discovering new topic partitions. Non-positive disables discovery. Only works when `topic-pattern` is used.                                              |
| subscription.name        | String  | Required for single-table mode or per multi-table item | -         | Consumer subscription name. Can be defined globally or per `tables_configs` item.                                                                                                        |
| client.service-url       | String  | Yes                                               | -             | Pulsar client service URL, for example `pulsar://localhost:6650`.                                                                                                                        |
| admin.service-url        | String  | Yes                                               | -             | Pulsar admin HTTP URL, for example `http://localhost:8080`.                                                                                                                              |
| auth.plugin-class        | String  | No                                                | -             | Pulsar client authentication plugin class name.                                                                                                                                          |
| auth.params              | String  | No                                                | -             | Pulsar client authentication parameters, for example `key1:val1,key2:val2`.                                                                                                              |
| poll.timeout             | Integer | No                                                | 100           | Timeout (ms) for polling messages from Pulsar.                                                                                                                                          |
| poll.interval            | Long    | No                                                | 50            | Interval (ms) between two polls.                                                                                                                                                        |
| poll.batch.size          | Integer | No                                                | 500           | Maximum number of messages fetched in a single poll.                                                                                                                                    |
| cursor.startup.mode      | Enum    | No                                                | LATEST        | Startup position mode. Options: `EARLIEST`, `LATEST`, `SUBSCRIPTION`, `TIMESTAMP`.                                                                                                       |
| cursor.startup.timestamp | Long    | No                                                | -             | Startup timestamp (ms) when `cursor.startup.mode = TIMESTAMP`.                                                                                                                           |
| cursor.reset.mode        | Enum    | Required when `cursor.startup.mode = SUBSCRIPTION` | -            | Reset mode when `cursor.startup.mode = SUBSCRIPTION`. Options: `EARLIEST`, `LATEST`.                                                                                                     |
| cursor.stop.mode         | Enum    | No                                                | NEVER         | Stop position mode. Options: `NEVER` (streaming), `LATEST` (batch), `TIMESTAMP` (batch).                                                                                                 |
| cursor.stop.timestamp    | Long    | No                                                | -             | Stop timestamp (ms) when `cursor.stop.mode = TIMESTAMP`.                                                                                                                                 |
| schema                   | Config  | No                                                | -             | Data structure, including field names and types. See [Schema Feature](../../introduction/concepts/schema-feature.md) for details.                                                        |
| format                   | String  | No                                                | json          | Data format. Supported values: `json`, `canal_json`, `avro`. Multi-table mode only supports `JSON`, `CANAL_JSON`, and `AVRO`. The `schema` option is required for `avro` format.        |
| field_delimiter          | String  | No                                                | ,             | Field delimiter for the `text` format.                                                                                                                                                  |
| common-options           |         | No                                                | -             | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md) for details.                                                                    |

### topic [String]

Topic name(s) to read data from when the table is used as source. Supports a
comma-separated list of topics, for example `'topic-1,topic-2'`.

**Note:** Only one of `topic`, `topic-pattern`, and `tables_configs` can be
specified for sources.

### topic-pattern [String]

Regular expression for a pattern of topic names to read from. All topics with
names that match the specified regular expression will be subscribed by the
consumer when the job starts running.

**Note:** Only one of `topic`, `topic-pattern`, and `tables_configs` can be
specified for sources.

### table_path [String]

Logical table identifier for one `tables_configs` item. This option is mainly
used in multi-table mode.

### tables_configs [Array]

Multi-table source configuration. Each item can override global defaults such
as `format`, cursor options, and `subscription.name`.

Each item must configure exactly one of:

- `topic`
- `topic-pattern`

Additional rules:

- `table_path` is required when `topic-pattern` is used.
- `subscription.name` must exist either globally or inside the item.
- Only `JSON`, `CANAL_JSON`, and `AVRO` are supported in multi-table mode.
- Explicit `topic` entries must not overlap with any `topic-pattern` entry.
- If multiple `topic-pattern` items can match the same topic, the first
  matching item in `tables_configs` wins. Put more specific patterns before
  broader ones.
- In batch mode, multi-table configurations must be bounded. If more than one
  table is configured and any table uses `cursor.stop.mode = NEVER`, the
  source is unbounded and batch jobs are rejected. Single-table mode and
  single-entry `tables_configs` keep backward-compatible batch behavior.

### topic-discovery.interval [Long]

The interval (in ms) for the Pulsar source to discover new topic partitions. A
non-positive value disables topic partition discovery.

**Note:** This option only works if `topic-pattern` is used.

### subscription.name [String]

The subscription name for this consumer.

For a single-table source, `subscription.name` is required. For a multi-table
source, it can be defined globally or inside each `tables_configs` item. If
configured in both places, the item-level value takes effect for that item.

### client.service-url [String]

Service URL provider for the Pulsar service. To connect to Pulsar using the
client libraries, specify a Pulsar protocol URL. Multiple URLs can be assigned
to specific clusters using the Pulsar scheme.

For example: `pulsar://localhost:6650,localhost:6651`.

### admin.service-url [String]

Pulsar service HTTP URL for the admin endpoint.

For example, `http://my-broker.example.com:8080`, or
`https://my-broker.example.com:8443` for TLS.

### auth.plugin-class [String]

Name of the authentication plugin.

### auth.params [String]

Parameters for the authentication plugin.

For example: `key1:val1,key2:val2`.

### poll.timeout [Integer]

Maximum time (in ms) to wait when fetching records. A longer time increases
throughput but also latency.

### poll.interval [Long]

Interval (in ms) between two polls. A shorter time increases throughput, but
also increases CPU load.

### poll.batch.size [Integer]

Maximum number of records fetched in one poll.

### cursor.startup.mode [Enum]

Startup mode for the Pulsar consumer. Valid values are `EARLIEST`, `LATEST`,
`SUBSCRIPTION`, and `TIMESTAMP`.

### cursor.startup.timestamp [Long]

Start from the specified epoch timestamp (in milliseconds).

**Note:** Required when `cursor.startup.mode = TIMESTAMP`.

### cursor.reset.mode [Enum]

Cursor reset strategy when `cursor.startup.mode = SUBSCRIPTION`. Valid values
are `EARLIEST` and `LATEST`. It has no default value and must be configured in
that mode.

### cursor.stop.mode [String]

Stop mode for the Pulsar consumer. Valid values are `NEVER`, `LATEST`, and
`TIMESTAMP`.

**Note:** When `NEVER` is specified, the job runs in streaming mode; the
other modes run in batch mode.

### cursor.stop.timestamp [Long]

Stop from the specified epoch timestamp (in milliseconds).

**Note:** Required when `cursor.stop.mode = TIMESTAMP`.

### schema [Config]

Data structure, including field names and types. See
[Schema Feature](../../introduction/concepts/schema-feature.md) for details.

### format [String]

Data format. The default format is `json`. Supported formats: `json`,
`canal_json`, and `avro`. The `schema` option is required for `avro` format.
See [formats](../formats) for more details.

### field_delimiter [String]

Field delimiter for the `text` format. The default value is `,`.

### common options

Source plugin common parameters, please refer to
[Source Common Options](../common-options/source-common-options.md) for
details.

## Task Example

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

### Read Canal JSON Messages

Use `format = canal_json` when the Pulsar topic stores Canal JSON change
events.

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

For batch jobs, use a bounded stop mode such as `LATEST` or `TIMESTAMP`. Use
`cursor.stop.mode = "NEVER"` for streaming jobs.

## Changelog

<ChangeLog />