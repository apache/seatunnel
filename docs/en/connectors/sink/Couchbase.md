import ChangeLog from '../changelog/connector-couchbase.md';

# Couchbase

> Couchbase Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Description

Writes data to a [Couchbase](https://www.couchbase.com/) collection.
Each incoming row is stored as a JSON document. The document key is built from the
`primary-key` fields using a **length-prefixed canonical encoding** (`<len>:<value>` components
separated by `#`, e.g. `3:foo#3:bar`). This encoding is collision-free: values that contain
separators (`#`) or other special characters cannot produce the same key as distinct tuples.
When no primary key is configured a random UUID is used.

The connector supports:

- **Upsert mode** — insert or replace existing documents.
- **Batch flushing** — buffer rows in memory and flush on size or time threshold.
- **Retry** — transient write failures are retried with **linear backoff** (attempt n waits
  `retry.interval × n` milliseconds).

## Supported DataSource Info

In order to use the Couchbase connector, the following dependency is required.
It can be downloaded from the Maven Central Repository.

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| Couchbase  | Server 7.x+        | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-couchbase) |

## Database Dependency

> Please install the connector plugin before running jobs:

```shell
sh bin/install-plugin.sh ${version}
```

## Data Type Mapping

Couchbase stores JSON documents. The connector maps SeaTunnel types to JSON values as follows:

| SeaTunnel Data Type | Couchbase JSON value |
|---------------------|----------------------|
| BOOLEAN             | Boolean              |
| TINYINT / SMALLINT / INT | Number (integer) |
| BIGINT              | Number (long)        |
| FLOAT / DOUBLE      | Number (floating point) |
| DECIMAL             | String (exact decimal, e.g. `"123.456"`) |
| STRING              | String               |
| DATE / TIME / TIMESTAMP | String (ISO-8601) |
| BYTES               | String (Base64-encoded) |
| ARRAY               | Array (elements recursively converted) |
| MAP                 | Object (keys coerced to String, values recursively converted) |
| ROW                 | Object (nested JSON document) |
| NULL                | null                 |

## Sink Options

| Name                  | Type          | Required | Default    | Description |
|-----------------------|---------------|----------|------------|-------------|
| connection.string     | String        | Yes      | -          | Couchbase connection string, e.g. `couchbase://localhost`. |
| username              | String        | Yes      | -          | Couchbase username. |
| password              | String        | Yes      | -          | Couchbase password. |
| bucket                | String        | Yes      | -          | Target bucket name. |
| scope                 | String        | No       | `_default` | Target scope name within the bucket. |
| collection            | String        | Yes      | -          | Target collection name. |
| primary-key           | `List<String>` | No       | -          | Field names used to build the document key (length-prefixed encoding: `<len>:<value>` components separated by `#`). A random UUID is used when not set. |
| upsert-enable         | Boolean       | No       | `false`    | Enable upsert (insert-or-replace) mode. When `false`, duplicate keys will cause an error. |
| buffer-flush.max-rows | Integer       | No       | `1000`     | Maximum rows to buffer before a batch write is triggered. Use `-1` to disable. |
| buffer-flush.interval | Long          | No       | `30000`    | Maximum milliseconds between batch writes. Use `-1` to disable. |
| retry.max             | Integer       | No       | `3`        | Maximum retry attempts on transient write failure. |
| retry.interval        | Long          | No       | `1000`     | Base milliseconds for linear retry delay. Attempt `n` waits `retry.interval × n` ms. |

## Task Example

### Simple example

```hocon
sink {
  Couchbase {
    connection.string = "couchbase://127.0.0.1"
    username          = "Administrator"
    password          = "password"
    bucket            = "my_bucket"
    collection        = "my_collection"
  }
}
```

### Upsert with composite document key

```hocon
sink {
  Couchbase {
    connection.string      = "couchbase://127.0.0.1"
    username               = "Administrator"
    password               = "password"
    bucket                 = "my_bucket"
    scope                  = "_default"
    collection             = "my_collection"
    primary-key            = ["user_id", "order_id"]
    upsert-enable          = true
    buffer-flush.max-rows  = 500
    buffer-flush.interval  = 10000
    retry.max              = 5
    retry.interval         = 2000
  }
}
```

<ChangeLog />
