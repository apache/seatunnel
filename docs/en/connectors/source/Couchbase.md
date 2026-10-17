import ChangeLog from '../changelog/connector-couchbase.md';

# Couchbase

> Couchbase source connector

## Description

Read JSON documents from one Couchbase collection using a bounded SQL++ query. The existing Couchbase sink is unchanged.

## Supported DataSource Info

| DataSource | Supported versions |
|------------|--------------------|
| Couchbase Server | 7.x |

## Key features

- [x] batch
- [ ] stream
- [ ] exactly-once
- [ ] parallelism
- [ ] schema evolution

The source requires `parallelism = 1`. It uses a single query and consumes rows with backpressure instead of loading the full result into memory. It does not partition the collection into splits or use `LIMIT`/`OFFSET` pagination.

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| connection.string | String | Yes | - | Couchbase SDK connection string, for example `couchbase://localhost`. Use `couchbases://` for TLS. |
| username | String | Yes | - | Couchbase username. |
| password | String | Yes | - | Couchbase password. |
| bucket | String | Yes | - | Bucket to read. |
| scope | String | No | _default | Scope within the bucket. |
| collection | String | Yes | - | Collection to read. |
| schema | Config | Yes | - | Explicit output schema; fields refer to document fields with matching names. |
| ready.timeout | Int | No | 30 | Bucket readiness timeout, in seconds. Must be positive. |
| query.timeout | Int | No | 300 | Collection query timeout, in seconds. Must be positive. |
| common-options | Config | No | - | See [Source Common Options](../common-options/source-common-options.md). |

## Prerequisites

Enable the Couchbase Data, Query and Index services. A suitable index must exist on the collection; a primary index supports the full collection scan. Index creation and data administration are not performed by this connector.

The configured account needs permission to query the selected collection. Grant only the required access; an administrator account is not required. For TLS, use `couchbases://`. The SDK trusts public CAs and the bundled Capella CA; for a private CA, set `security.trustCertificate=/path/to/ca.pem` as a connection-string parameter and make that file available on every worker. Certificate hostname verification remains enabled.

## Data mapping

The source selects each document body, without document IDs or metadata, and uses SeaTunnel's JSON format conversion with the configured schema. Field matching is case-sensitive. Fields not in the schema are ignored. Missing fields and explicit JSON null values become null. Invalid values fail the read rather than being silently skipped.

JSON strings, booleans, numbers, arrays and nested objects are converted to their configured SeaTunnel types using the JSON format's rules. Declare nested objects as ROW and arrays as ARRAY; no schema inference is performed. Non-object JSON documents and binary documents are not supported.

Numeric values are preserved from the SQL++ response without an additional floating-point conversion in the connector. Couchbase itself can round numeric values during query evaluation; declaring DECIMAL cannot recover precision already lost by the server. Store exact high-precision decimal values as JSON strings when necessary.

## Consistency and recovery

The query is read-only and uses `REQUEST_PLUS` so that indexes catch up with mutations preceding the query. This is not a transactionally consistent snapshot: concurrent updates may affect the results. Quiesce writes if the job requires a stable export.

The source checkpoints no document offset. On failure or restoration, it starts a new collection query from the beginning. Rows already emitted may be replayed, and concurrent collection changes can alter the new result. Use an idempotent downstream write strategy when replay is possible. This source does not provide exactly-once delivery, CDC, or incremental reads.

Cancellation stops an in-flight query, and reader closure releases the SDK connection. If cancellation occurs during the synchronous initial bucket-readiness check, that check can still run until `ready.timeout` (30 seconds by default). When the reader observes cancellation after readiness, opening fails and releases the connection. Query, authentication and conversion failures fail the job; they do not report a successfully completed collection.

## Example

Create a primary index for the collection before running the job:

```sql
CREATE PRIMARY INDEX ON `example`.`_default`.`users`;
```

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Couchbase {
    connection.string = "couchbase://localhost"
    username = "reader"
    password = ${COUCHBASE_PASSWORD}
    bucket = "example"
    scope = "_default"
    collection = "users"
    schema {
      fields {
        id = INT
        name = STRING
        active = BOOLEAN
        score = DOUBLE
      }
    }
  }
}
sink {
  Console {}
}
```

## Changelog

<ChangeLog />
