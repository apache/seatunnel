import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Source Connector

`Source: HugeGraph`

## Description

The HugeGraph source connector reads graph data from Apache HugeGraph through the HugeGraph REST API.

It performs a bounded scan of one vertex label or one edge label and checkpoints its progress so a job can resume after failover.

- At `parallelism = 1` it pages the label via the server-side list API, following HugeGraph page markers until the server returns `page = null`. Server-side `filter` (property-equality) is applied in this mode.
- At `parallelism > 1` it splits the keyspace into shards (via the HugeGraph `traverser().vertexShards / edgeShards` API) and scans them across parallel readers. Because the shard scan is by key range and returns all labels, the connector filters to the configured `label` client-side. See [Parallel read](#parallel-read).

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

## Options

| Name               | Type    | Required | Default  | Description |
|--------------------|---------|----------|----------|-------------|
| `host`             | String  | Yes      | -        | HugeGraph server host. |
| `port`             | Integer | Yes      | -        | HugeGraph server port. |
| `protocol`         | String  | No       | `http`   | Server protocol: `http` or `https`. HTTPS uses the JVM trust store. |
| `graph_name`       | String  | Yes      | -        | HugeGraph graph name. |
| `label`            | String  | Yes      | -        | Vertex label or edge label to read. |
| `schema`           | Object  | No       | -        | Output property columns declared with `schema.fields`. Reserved graph columns are added by the connector. **When omitted, the connector auto-discovers all property columns of `label` from the server (types inferred, columns ordered by name).** See [Schema auto-discovery](#schema-auto-discovery). |
| `label_type`       | Enum    | No       | `VERTEX` | Label type. Supported values: `VERTEX`, `EDGE`. |
| `page_size`        | Integer | No       | `1000`   | Number of records per HugeGraph page. Must be in range `[100, 10000]`. |
| `split_size`       | Long    | No       | `1048576` | Target size in bytes of each key-range shard when `parallelism > 1`. A larger value yields fewer, bigger shards. Ignored at `parallelism = 1`. Requires a scan-capable backend (RocksDB / HBase / Cassandra). |
| `filter`           | Map     | No       | -        | Optional property equality conditions applied server-side, for example `{ country = "US", active = "true" }`. Only elements whose properties match all entries are returned. Every key must be a property of `label`; an unknown key fails at startup. When omitted, all elements of the label are read. **Cannot be combined with `parallelism > 1`** (the shard scan cannot push property filters server-side); the job fails at startup if both are set. |
| `time_zone`        | String  | No       | Worker JVM default | ZoneId used to convert HugeGraph DATE epoch values, for example `UTC` or `Asia/Shanghai`. Set it explicitly when workers may use different JVM time zones. |
| `graph_space`      | String  | No       | `DEFAULT` | The graph space the graph belongs to. |
| `username`         | String  | No       | -        | HugeGraph username. |
| `password`         | String  | No       | -        | HugeGraph password. |
| `max_retries`      | Integer | No       | `3`      | Retries after the initial attempt. Set to `0` to disable retries. |
| `retry_backoff_ms` | Integer | No       | `5000`   | Base backoff between retries in ms. Grows exponentially per attempt (`retry_backoff_ms * 2^(attempt-1)`), capped at `retry_backoff_max_ms`. |
| `retry_backoff_max_ms` | Integer | No   | `30000`  | Upper bound in ms for the exponential retry backoff. |

## Output Schema

Vertex output columns:

```text
~id, ~label, <schema.fields columns...>
```

Edge output columns:

```text
~id, ~label, ~source_id, ~source_label, ~target_id, ~target_label, <schema.fields columns...>
```

Columns prefixed with `~` are reserved columns added by the connector. HugeGraph property keys cannot start with `~`, so they do not conflict with user properties.

## Type Mapping

`schema.fields` must match the HugeGraph property key type. The connector validates this before reading.

| HugeGraph type | SeaTunnel type |
|----------------|----------------|
| `TEXT`         | `STRING`       |
| `INT`          | `INT`          |
| `LONG`         | `BIGINT`       |
| `FLOAT`        | `FLOAT`        |
| `DOUBLE`       | `DOUBLE`       |
| `BOOLEAN`      | `BOOLEAN`      |
| `DATE`         | `TIMESTAMP`    |
| `UUID`         | `STRING`       |
| `BLOB`         | `BYTES`        |

### Multi-valued (LIST / SET) properties

A HugeGraph property whose cardinality is `LIST` or `SET` is read as a SeaTunnel `ARRAY`. Declare it in `schema.fields` as `array<T>`, where `T` is the SeaTunnel type of the element (from the table above). For example, a `LIST<TEXT>` property named `tags` is declared as `tags = "array<string>"`.

Notes:

- `SET` elements have no guaranteed order on the server; use `LIST` when order matters.
- If a property has cardinality `LIST`/`SET` on the server but is declared as a scalar (or vice versa), the job fails at startup with a message telling you the correct declaration.
- `BLOB` elements inside a `LIST`/`SET` are not supported.

## Example

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    page_size = 1000
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}
```

## Schema auto-discovery

`schema` is optional. When omitted, the connector connects to the server at job build time, reads the definition of `label`, and produces one output column per property key (types from the [Type Mapping](#type-mapping) table, `LIST`/`SET` as `array<T>`), ordered by property name. This is convenient for a full-label dump when you do not want to hand-declare every field.

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    # no schema block: all properties of "person" are read
  }
}
```

Notes:

- The label must already exist on the server, otherwise the job fails at build time.
- A label with no property keys produces only the reserved columns (`~id`, `~label`, …).
- Declare `schema.fields` explicitly when you want to read only a subset of properties, fix the column order, or pin the types.

## Parallel read

For large graphs, set `parallelism > 1` to read a label in parallel. The enumerator asks HugeGraph to split the label's keyspace into shards of roughly `split_size` bytes and distributes them round-robin across readers, so throughput scales with parallelism instead of being bound by a single paging cursor.

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    parallelism = 8
    split_size = 1048576
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}
```

Notes:

- Shard scans require a scan-capable backend (RocksDB / HBase / Cassandra). The `memory` backend does not support shard splitting; use `parallelism = 1` there.
- A shard scan returns elements of all labels in the key range; the connector keeps only the configured `label`. On a graph where the target label is a small fraction of the data, a single-parallelism `filter`ed read may move less data even though it does not parallelize.
- `filter` is not supported with `parallelism > 1`; keep `parallelism = 1` to use a server-side filter, or drop the filter to read in parallel.
- Tune `split_size`: a smaller value yields more, smaller shards (finer load balancing, more requests); a larger value yields fewer, bigger shards.

## Changelog

<ChangeLog />
