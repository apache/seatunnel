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

## Options

| Name                     | Type    | Required | Default Value | Description                                                                                                      |
|--------------------------|---------|----------|---------------|------------------------------------------------------------------------------------------------------------------|
| topic                    | String  | No       | -             | Topic name(s) to read. Supports comma-separated list. **Note: only one of `topic`, `topic-pattern`, `tables_configs`** |
| topic-pattern            | String  | No       | -             | Regular expression for topic names. **Note: only one of `topic`, `topic-pattern`, `tables_configs`**            |
| table_path               | String  | No       | -             | Logical table identifier for multi-table mode                                                                    |
| tables_configs           | Array   | No       | -             | Multi-table configuration. Each item can override global defaults. **Note: only one of `topic`, `topic-pattern`, `tables_configs`** |
| topic-discovery.interval | Long    | No       | -1            | Interval (ms) to discover new partitions. Non-positive disables discovery. Only works with `topic-pattern`      |
| subscription.name        | String  | No       | -             | Consumer subscription name. Can be defined globally or per item in multi-table mode                              |
| client.service-url       | String  | Yes      | -             | Pulsar client service URL, e.g., `pulsar://localhost:6650`                                                      |
| admin.service-url        | String  | Yes      | -             | Pulsar admin HTTP URL, e.g., `http://localhost:8080`                                                            |
| auth.plugin-class        | String  | No       | -             | Pulsar client authentication plugin class name                                                                   |
| auth.params              | String  | No       | -             | Pulsar client authentication parameters                                                                          |
| poll.timeout             | Integer | No       | 100           | Timeout (ms) for polling messages from Pulsar                                                                    |
| poll.interval            | Long    | No       | 50            | Interval (ms) between two polls                                                                                  |
| poll.batch.size          | Integer | No       | 500           | Maximum number of messages to poll in a single batch                                                             |
| cursor.startup.mode      | Enum    | No       | LATEST        | Startup position mode. Options: `EARLIEST`, `LATEST`, `SUBSCRIPTION`, `TIMESTAMP`                                |
| cursor.startup.timestamp | Long    | No       | -             | Start timestamp (ms) when `cursor.startup.mode=TIMESTAMP`                                                        |
| cursor.reset.mode        | Enum    | No       | LATEST        | Reset mode when `cursor.startup.mode=SUBSCRIPTION`. Options: `EARLIEST`, `LATEST`                               |
| cursor.stop.mode         | Enum    | No       | NEVER         | Stop position mode. Options: `NEVER` (streaming), `LATEST` (batch), `TIMESTAMP` (batch)                         |
| cursor.stop.timestamp    | Long    | No       | -             | Stop timestamp (ms) when `cursor.stop.mode=TIMESTAMP`                                                            |
| schema                   | Config  | No       | -             | Data structure including field names and types                                                                   |
| format                   | String  | No       | json          | Data format. Default is json. **Multi-table mode only supports JSON and CANAL_JSON**                            |
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
- Only `JSON` and `CANAL_JSON` are supported in multi-table mode.
- Explicit `topic` entries must not overlap with any `topic-pattern` entry.
- If multiple `topic-pattern` items can match the same topic, the first matching item in `tables_configs` wins. Put more specific patterns before broader ones.
- In batch mode, multi-table configurations must be bounded. If more than one table is configured and any table uses `cursor.stop.mode = NEVER`, the source is unbounded and batch jobs are rejected. Single-table mode and single-entry `tables_configs` keep backward-compatible batch behavior.

### topic-discovery.interval [Long]

The interval (in ms) for the Pulsar source to discover the new topic partitions. A non-positive value disables the topic partition discovery.

**Note, This option only works if the 'topic-pattern' option is used.**

### subscription.name [String]

Specify the subscription name for this consumer. This is required for each effective table configuration, but in multi-table mode it can be defined globally or overridden per `tables_configs` item.

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

The interval time(in ms) when fetcing records. A shorter time increases throughput, but also increases CPU load.

### poll.batch.size [Integer]

The maximum number of records to fetch to wait when polling. A longer time increases throughput but also latency.

### cursor.startup.mode [Enum]

Startup mode for Pulsar consumer, valid values are `'EARLIEST'`, `'LATEST'`, `'SUBSCRIPTION'`, `'TIMESTAMP'`.

### cursor.startup.timestamp [Long]

Start from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "cursor.startup.mode" option used `'TIMESTAMP'`.**

### cursor.reset.mode [Enum]

Cursor reset strategy for Pulsar consumer valid values are `'EARLIEST'`, `'LATEST'`.

**Note, This option only works if the "cursor.startup.mode" option used `'SUBSCRIPTION'`.**

### cursor.stop.mode [String]

Stop mode for Pulsar consumer, valid values are `'NEVER'`, `'LATEST'`and `'TIMESTAMP'`.

**Note, When `'NEVER' `is specified, it is a real-time job, and other mode are off-line jobs.**

### cursor.stop.timestamp [Long]

Stop from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "cursor.stop.mode" option used `'TIMESTAMP'`.**

### schema [Config]

The structure of the data, including field names and field types.
reference to [Schema-Feature](../../introduction/concepts/schema-feature.md)

## format [String]

Data format. The default format is json, reference [formats](../formats).

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

```hocon
source {
  Pulsar {
    topic = "example"
    subscription.name = "seatunnel"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://my-broker.example.com:8080"
    plugin_output = "test"
  }
}
```

## Multi-table Example

```hocon
source {
  Pulsar {
    subscription.name = "seatunnel-sub"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "NEVER"
    format = "json"

    tables_configs = [
      {
        table_path = "default.orders"
        topic = "persistent://public/default/orders"
        schema = {
          fields {
            order_id = "bigint"
            user_id = "int"
          }
        }
      },
      {
        table_path = "default.users"
        topic-pattern = "persistent://public/default/users-.*"
        subscription.name = "users-sub"
        format = "canal_json"
        schema = {
          fields {
            user_id = "int"
            name = "string"
          }
        }
      }
    ]
  }
}
```

In batch mode, replace `cursor.stop.mode = "NEVER"` with a bounded mode such as `LATEST` or `TIMESTAMP`.

## Changelog

<ChangeLog />
