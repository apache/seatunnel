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
| retry.max             | Integer       | No       | `3`        | Maximum retry attempts on transient write failure. |
| retry.interval        | Long          | No       | `1000`     | Base milliseconds for linear retry delay. Attempt `n` waits `retry.interval × n` ms. |

## Security

### TLS / encrypted transport

For production deployments, use the `couchbases://` scheme (note the trailing **s**) to enable
TLS. Pass the CA certificate or a custom trust store through the Couchbase Java SDK's
`ClusterEnvironment`:

```hocon
sink {
  Couchbase {
    # Use couchbases:// (with trailing 's') for TLS-encrypted transport
    connection.string = "couchbases://couchbase.example.com"
    username          = "seatunnel_writer"
    password          = "${env:COUCHBASE_PASSWORD}"
    bucket            = "my_bucket"
    collection        = "my_collection"
  }
}
```

Refer to the
[Couchbase Java SDK — Secure Connections](https://docs.couchbase.com/java-sdk/current/howtos/managing-connections.html#ssl)
for TLS configuration, certificate pinning, client certificates, and cipher suite options.

### Least-privilege service account

Do **not** use the built-in `Administrator` account in production.
Create a dedicated Couchbase user with the minimal required role:

- `Data Writer` on the target bucket/scope/collection (for insert-only workloads).
- `Data Reader` + `Data Writer` (for upsert workloads that may need to read-before-write).

### Credential protection

Avoid storing passwords in plain-text job configuration files.
SeaTunnel supports encrypted configuration values — see the
[SeaTunnel credential encryption documentation](../../introduction/configuration/config-encryption-decryption.md)
for details on substituting secrets at runtime.

## Task Example

### Simple example *(development only)*

> ⚠️ The connection string and credentials below are for **local development only**.
> See the [Security](#security) section above before deploying to production.

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

### Upsert with composite document key *(development only)*

> ⚠️ The connection string and credentials below are for **local development only**.
> See the [Security](#security) section above before deploying to production.

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
    retry.max              = 5
    retry.interval         = 2000
  }
}
```

### Timer Flush

The sink can flush its buffer on a timer so that buffered rows are written even when the upstream  
flow is idle and fewer than `buffer-flush.max-rows` rows have been buffered. This timer is driven  
by the engine, not by the connector, and is currently supported only by **SeaTunnel Zeta**.

Enable it by setting `sink.flush.interval` (milliseconds) in the job `env` block:

```hocon  
env {  
sink.flush.interval = 10000  
}  
```

> On Spark and Flink there is no sub-checkpoint timer flush: `sink.flush.interval` is a Zeta engine  
> primitive, and the Spark/Flink sink writer context does not implement it. On those engines the  
> buffer is flushed when it reaches `buffer-flush.max-rows`, on checkpoint (`CouchbaseWriter`  
> flushes in `prepareCommit()`), and when the writer is closed. For lower latency between  
> checkpoints on Spark or Flink, tune `buffer-flush.max-rows` accordingly.


<ChangeLog />
